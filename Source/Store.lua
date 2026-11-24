-- Applying rows to spaces and keeping the replication position

local fiber   = require('fiber')
local msgpack = require('msgpack')
local key_def = require('key_def')
local log     = require('log')

local protocol = require('MirrorFlayer.Protocol')

local NULL = box.NULL

-- Targets --------------------------------------------------------------------

local Target = {}
Target.__index = Target

-- spec is a space name or function (operation, old, new); columns are the
-- source column names in tuple order, by default the names of the space format
local function target(spec, columns)
  local self = setmetatable({}, Target)
  if type(spec) == 'function' then
    self.call = spec
    self.name = tostring(spec)
    if columns == nil then
      protocol.fatal('a function target needs a column list')
    end
  else
    local space = box.space[spec]
    if space == nil then
      protocol.fatal('space %s does not exist', tostring(spec))
    end
    if space.index[0] == nil then
      protocol.fatal('space %s has no primary index', spec)
    end
    self.space = space
    self.name = spec
    if columns == nil then
      columns = {}
      for index, field in ipairs(space:format()) do
        columns[index] = field.name
      end
      if #columns == 0 then
        protocol.fatal('space %s has no format, give the column list', spec)
      end
    end
    self.parts = space.index[0].parts
    self.key = {}
    for index, part in ipairs(self.parts) do
      if type(part.fieldno) ~= 'number' or part.fieldno > #columns then
        protocol.fatal('space %s: key part %d is not among the mapped columns', spec, index)
      end
      self.key[index] = part.fieldno
    end
    self.definition = key_def.new(self.parts)
  end
  self.columns = columns
  return self
end

-- Resolves column names against the source table columns
function Target:bind(columns)
  local positions = {}
  for index, column in ipairs(columns) do
    positions[column.name] = index
  end
  self.positions = {}
  for index, name in ipairs(self.columns) do
    local position = positions[name]
    if position == nil then
      protocol.fatal('target %s: column %s is missing in the source table', self.name, name)
    end
    if not columns[position].supported then
      protocol.fatal('target %s: column %s has unsupported type %s', self.name, name, columns[position].type)
    end
    self.positions[index] = position
  end
end

function Target:tuple(row)
  local tuple = {}
  for index, position in ipairs(self.positions) do
    local value = row[position]
    if value == nil then
      value = NULL
    end
    tuple[index] = value
  end
  return tuple
end

function Target:keyof(tuple)
  local key = {}
  for index, fieldno in ipairs(self.key) do
    key[index] = tuple[fieldno]
  end
  return key
end

local function same(left, right)
  for index = 1, #left do
    if left[index] ~= right[index] then
      return false
    end
  end
  return true
end

-- Applies one change: operation 'insert' | 'update' | 'delete', old and new
-- are source rows. Runs inside a transaction and must not yield.
function Target:apply(operation, old, new)
  local before = old ~= nil and self:tuple(old) or nil
  local after  = new ~= nil and self:tuple(new) or nil
  if self.call ~= nil then
    self.call(operation, before, after)
    return
  end
  if operation == 'insert' then
    self.space:replace(after)
  elseif operation == 'update' then
    local key = self:keyof(before)
    if not same(key, self:keyof(after)) then
      self.space:delete(key)
    end
    self.space:replace(after)
  else
    self.space:delete(self:keyof(before))
  end
end

-- Transactions -----------------------------------------------------------------

-- A yield inside a transaction aborts it with this code, also in triggers:
-- on_replace triggers that wait on a full channel make writes fail so
local YIELD = box.error.TRANSACTION_YIELD or 154

local function trace(problem)
  if type(problem) == 'table' or
     (type(problem) == 'cdata' and problem.code ~= nil) then
    return problem
  end
  return debug.traceback(tostring(problem), 2)
end

local function transaction(action)
  box.begin()
  local status, problem = xpcall(action, trace)
  if not status then
    if box.is_in_txn() then
      box.rollback()
    end
    error(problem, 0)
  end
  box.commit()
end

local function yielded(problem)
  return type(problem) == 'cdata' and problem.code == YIELD
end

-- Runs perform(statement) for every statement and then final() in one
-- transaction. When a trigger yields and aborts it, runs each statement in a
-- transaction of its own, repeating it after a pause until its triggers pass
-- (applying is idempotent), and final() last. Returns true when atomic.
local function apply(statements, perform, final)
  local status, problem = pcall(transaction, function ()
    for _, statement in ipairs(statements) do
      perform(statement)
    end
    if final ~= nil then
      final()
    end
  end)
  if status then
    return true
  end
  if not yielded(problem) then
    error(problem, 0)
  end
  for _, statement in ipairs(statements) do
    local delay = 0.001
    while true do
      local done, failure = pcall(transaction, function ()
        perform(statement)
      end)
      if done then
        break
      end
      if not yielded(failure) then
        error(failure, 0)
      end
      fiber.sleep(delay)
      delay = math.min(delay * 2, 0.1)
    end
  end
  if final ~= nil then
    transaction(final)
  end
  return false
end

-- Full load ------------------------------------------------------------------

-- Loads a snapshot of a table into its targets: rows are written over the
-- current content in batches, rows missing from the snapshot are deleted
-- afterwards, so the space never goes empty. Rows equal to the stored tuples
-- are not written, so their triggers do not fire. Function targets get
-- 'reset' first and then 'insert' for each row.
local Load = {}
Load.__index = Load

local function load(targets, batch)
  local self = setmetatable({ targets = targets, batch = batch, pending = {}, seen = {}, written = 0 }, Load)
  for index, item in ipairs(targets) do
    if item.call ~= nil then
      transaction(function ()
        item.call('reset', nil, nil)
      end)
    else
      self.seen[index] = {}
    end
  end
  return self
end

function Load:write(row)
  for index, item in ipairs(self.targets) do
    local tuple = item:tuple(row)
    if item.call ~= nil then
      item.call('insert', nil, tuple)
    else
      local stored = item.space:get(item:keyof(tuple))
      if stored == nil or msgpack.encode(stored) ~= msgpack.encode(tuple) then
        stored = item.space:replace(tuple)
        self.written = self.written + 1
      end
      self.seen[index][msgpack.encode(item.definition:extract_key(stored))] = true
    end
  end
end

function Load:flush()
  local rows = self.pending
  if #rows == 0 then
    return
  end
  self.pending = {}
  if not apply(rows, function (row) self:write(row) end) then
    self.split = true
  end
  fiber.yield()
end

function Load:add(row)
  table.insert(self.pending, row)
  if #self.pending >= self.batch then
    self:flush()
  end
end

-- Deletes rows that were not in the snapshot
function Load:finish()
  self:flush()
  local deleted = 0
  for index, item in ipairs(self.targets) do
    if item.call == nil then
      local seen  = self.seen[index]
      local stale = {}
      local count = 0
      for _, tuple in item.space:pairs() do
        local key = item.definition:extract_key(tuple)
        if not seen[msgpack.encode(key)] then
          table.insert(stale, key)
        end
        count = count + 1
        if count % self.batch == 0 then
          fiber.yield()
        end
      end
      for first = 1, #stale, self.batch do
        local keys = {}
        for position = first, math.min(first + self.batch - 1, #stale) do
          table.insert(keys, stale[position])
        end
        apply(keys, function (key) item.space:delete(key) end)
        fiber.yield()
      end
      deleted = deleted + #stale
    end
  end
  return deleted
end

-- Position -------------------------------------------------------------------

local Position = {}
Position.__index = Position

local NUMERIC = { unsigned = true, integer = true, number = true }

-- Key of the Python replicator, which kept [0, gtid, 0] in ReplicatorPositions
local LEGACY = 0

-- Opens the position space, created like Registry creates ReplicatorPositions
-- when missing. key is a stable number or string of this replicator, the
-- replica server id by default; the tuple is { key, gtid_state, event_time }.
local function position(space_name, key, name)
  local space = box.space[space_name]
  if space == nil then
    space = box.schema.space.create(space_name, { if_not_exists = true })
    space:create_index('ID', { if_not_exists = true, unique = true,
      parts = { 1, type(key) == 'number' and 'unsigned' or 'string' } })
  end
  if space.index[0] == nil then
    protocol.fatal('position space %s has no primary index', space_name)
  end
  local part = space.index[0].parts[1].type
  if (part == 'string' and type(key) ~= 'string') or (NUMERIC[part] and type(key) ~= 'number') then
    protocol.fatal('position space %s has a %s key, the key %s does not fit it', space_name, part, tostring(key))
  end
  if key == LEGACY then
    protocol.fatal('position key 0 belongs to the Python replicator, choose another one')
  end
  return setmetatable({ space = space, key = key, name = name }, Position)
end

-- Position of the Python replicator: key 0 in this space or in ReplicatorPositions
function Position:legacy()
  for _, space in ipairs({ self.space, box.space.ReplicatorPositions }) do
    local status, tuple = pcall(space.get, space, LEGACY)
    if status and tuple ~= nil and type(tuple[2]) == 'string' and tuple[2] ~= '' then
      log.info('MirrorFlayer[%s]: continuing from the position of the Python replicator %s', self.name, tuple[2])
      return tuple[2]
    end
  end
  return nil
end

-- Returns the stored GTID state or nil
function Position:get()
  local tuple = self.space:get(self.key)
  if tuple ~= nil and type(tuple[2]) == 'string' then
    return tuple[2]
  end
  return self:legacy()
end

-- Stores the GTID state; call inside the transaction of the applied changes
function Position:put(state, time)
  self.space:replace({ self.key, state, time or 0 })
end

return {
  target = target,
  load = load,
  transaction = transaction,
  apply = apply,
  position = position
}
