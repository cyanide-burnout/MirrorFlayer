-- Applying rows to spaces and keeping the replication position

local fiber = require('fiber')
local msgpack = require('msgpack')
local key_def = require('key_def')
local log = require('log')

local protocol = require('mirrorflayer.protocol')

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
  local after = new ~= nil and self:tuple(new) or nil
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

-- Full load ------------------------------------------------------------------

-- Loads a snapshot of a table into its targets: rows are written over the
-- current content in batches, rows missing from the snapshot are deleted
-- afterwards, so the space never goes empty. Function targets get 'reset'
-- first and then 'insert' for each row.
local Load = {}
Load.__index = Load

local function load(targets, batch)
  local self = setmetatable({ targets = targets, batch = batch, pending = {}, seen = {} }, Load)
  for index, item in ipairs(targets) do
    if item.call ~= nil then
      box.begin()
      local status, problem = pcall(item.call, 'reset', nil, nil)
      if not status then
        box.rollback()
        error(problem, 0)
      end
      box.commit()
    else
      self.seen[index] = {}
    end
  end
  return self
end

local function trace(problem)
  if type(problem) == 'table' then
    return problem
  end
  return debug.traceback(tostring(problem), 2)
end

local function transaction(action)
  box.begin()
  local status, problem = xpcall(action, trace)
  if not status then
    box.rollback()
    error(problem, 0)
  end
  box.commit()
end

function Load:flush()
  local rows = self.pending
  if #rows == 0 then
    return
  end
  self.pending = {}
  transaction(function ()
    for _, row in ipairs(rows) do
      for index, item in ipairs(self.targets) do
        if item.call ~= nil then
          item.call('insert', nil, item:tuple(row))
        else
          local stored = item.space:replace(item:tuple(row))
          self.seen[index][msgpack.encode(item.definition:extract_key(stored))] = true
        end
      end
    end
  end)
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
      local seen = self.seen[index]
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
        transaction(function ()
          for position = first, math.min(first + self.batch - 1, #stale) do
            item.space:delete(stale[position])
          end
        end)
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

-- Opens (creates when missing) the position space. The key is the replicator
-- name; a space with a numeric primary key is the layout of the Python
-- replicator, there the position lives under key 0.
local function position(space_name, name)
  local space = box.space[space_name]
  if space == nil then
    space = box.schema.space.create(space_name, {
      if_not_exists = true,
      format = { { 'Name', 'string' }, { 'GTID', 'string' }, { 'Time', 'number' } }
    })
    space:create_index('primary', { if_not_exists = true, parts = { 1, 'string' } })
  end
  if space.index[0] == nil then
    protocol.fatal('position space %s has no primary index', space_name)
  end
  local self = setmetatable({ space = space, name = name }, Position)
  local part = space.index[0].parts[1]
  if NUMERIC[part.type] then
    self.key = 0
    self.legacy = true
    log.warn('mirrorflayer[%s]: %s has a numeric key, keeping the position under key 0', name, space_name)
  else
    self.key = name
  end
  return self
end

-- Returns the stored GTID state or nil
function Position:get()
  local tuple = self.space:get(self.key)
  if tuple ~= nil and not self.legacy and type(tuple[2]) == 'string' then
    return tuple[2]
  end
  if tuple == nil and not self.legacy then
    -- Position of the Python replicator: [0, gtid, 0] in ReplicatorPositions
    local legacy = box.space.ReplicatorPositions
    if legacy ~= nil and legacy ~= self.space then
      local status, result = pcall(legacy.get, legacy, 0)
      if status and result ~= nil then
        log.info('mirrorflayer[%s]: continuing from the position of the Python replicator', self.name)
        tuple = result
      end
    end
  end
  if tuple == nil or type(tuple[2]) ~= 'string' or tuple[2] == '' then
    return nil
  end
  return tuple[2]
end

-- Stores the GTID state; call inside the transaction of the applied changes
function Position:put(state, time)
  self.space:replace({ self.key, state, time or 0 })
end

return {
  target = target,
  load = load,
  transaction = transaction,
  position = position
}
