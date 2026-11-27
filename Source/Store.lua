-- Applying rows to spaces and keeping the replication position

local fiber   = require('fiber')
local msgpack = require('msgpack')
local key_def = require('key_def')
local log     = require('log')

local log     = require('log')
local errors  = require('MirrorFlayer.Failure')
local convert = require('MirrorFlayer.Column')

local NULL      = box.NULL
local UNCHANGED = convert.UNCHANGED

-- Targets --------------------------------------------------------------------

local Target   = {}
Target.__index = Target

-- spec is a space name or function (operation, old, new); columns are the
-- source column names in tuple order, by default the names of the space format
local function target(spec, columns)
  local self = setmetatable({}, Target)
  if type(spec) == 'function' then
    self.call = spec
    self.name = tostring(spec)
    if columns == nil then
      errors.fatal('a function target needs a column list')
    end
  else
    local space = box.space[spec]
    if space == nil then
      -- Not fatal: the space may be made later, the replicator retries
      error(errors.failure(string.format('space %s does not exist yet', tostring(spec))), 0)
    end
    if space.index[0] == nil then
      errors.fatal('space %s has no primary index', spec)
    end
    self.space = space
    self.name  = spec
    if columns == nil then
      columns = {}
      for index, field in ipairs(space:format()) do
        columns[index] = field.name
      end
      if #columns == 0 then
        errors.fatal('space %s has no format, give the column list', spec)
      end
    end
    self.parts = space.index[0].parts
    self.key   = {}
    for index, part in ipairs(self.parts) do
      if type(part.fieldno) ~= 'number' or part.fieldno > #columns then
        errors.fatal('space %s: key part %d is not among the mapped columns', spec, index)
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
      errors.fatal('target %s: column %s is missing in the source table', self.name, name)
    end
    if not columns[position].supported then
      errors.fatal('target %s: column %s has unsupported type %s', self.name, name, columns[position].type)
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

-- Fields a change did not send take the values of the stored tuple, or
-- NULL when there is none (function targets get NULL); a patch is applied to
-- the stored value
function Target:complete(after, before)
  local stored
  for index = 1, #after do
    local field = after[index]
    if rawequal(field, UNCHANGED) or convert.patched(field) then
      if stored == nil and self.space ~= nil then
        stored = self.space:get(self:keyof(before or after)) or false
      end
      local value
      if stored then
        value = stored[index]
      end
      if value == nil then
        value = NULL
      end
      if convert.patched(field) then
        value = require(field.module).patch(value, field.diffs, field.mode)
      end
      after[index] = value
    end
  end
end

-- Applies one change: operation 'insert' | 'update' | 'delete', old and new
-- are source rows. Runs inside a transaction and must not yield.
function Target:apply(operation, old, new)
  local before = old ~= nil and self:tuple(old) or nil
  local after  = new ~= nil and self:tuple(new) or nil
  if after ~= nil then
    self:complete(after, before)
  end
  if before ~= nil then
    for index = 1, #before do
      if rawequal(before[index], UNCHANGED) then
        before[index] = NULL
      end
    end
  end
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

-- Pace of writes. The fiber yields after every transaction of at most batch
-- statements, so fibers reading the channels that triggers write to keep up.
-- A transaction a yielding trigger aborts nevertheless halves the statements
-- per transaction and pauses once so the channels drain; after a minute
-- without aborts the pace is back to batch.
-- A storm of aborts is avoided on purpose: Tarantool 3.9 was seen to crash in
-- the allocator after a series of them.
local CALM  = 60
local PAUSE = 0.01

local pace = { limit = 100, ceiling = 100, size = 100, streak = 0, calm = 0, aborts = 0 }

local function configure(batch)
  pace.limit   = batch
  pace.ceiling = batch
  pace.size    = batch
end

local function aborted(count)
  pace.aborts  = pace.aborts + 1
  pace.ceiling = math.max(1, math.floor(count / 2))
  pace.size    = pace.ceiling
  pace.streak  = 0
  pace.calm    = fiber.clock() + CALM
  log.warn('MirrorFlayer: a trigger yielded, %d statements per transaction for a while', pace.size)
end

local function succeeded()
  if pace.ceiling < pace.limit and fiber.clock() > pace.calm then
    pace.ceiling = pace.limit
  end
  pace.streak = pace.streak + 1
  if pace.streak >= 10 and pace.size < pace.ceiling then
    pace.size   = math.min(pace.ceiling, pace.size * 2)
    pace.streak = 0
  end
end

-- Runs perform(statement) for every statement and then final(), in
-- transactions of at most pace.size statements, final() in the last one.
-- A transaction a trigger aborts is repeated smaller after a pause (applying
-- is idempotent). Returns true when everything went in one transaction.
local function apply(statements, perform, final)
  local count  = #statements
  local first  = 1
  local chunks = 0
  repeat
    local last = math.min(count, first + pace.size - 1)
    local status, problem = pcall(transaction, function ()
      for index = first, last do
        perform(statements[index])
      end
      if last == count and final ~= nil then
        final()
      end
    end)
    if status then
      chunks = chunks + 1
      first  = last + 1
      succeeded()
      fiber.yield()
    elseif yielded(problem) then
      aborted(last - first + 1)
      fiber.sleep(PAUSE)
    else
      error(problem, 0)
    end
  until first > count
  return chunks == 1
end

-- Full load ------------------------------------------------------------------

-- Loads a snapshot of a table into its targets: rows are written over the
-- current content in batches, rows missing from the snapshot are deleted
-- afterwards, so the space never goes empty. Rows equal to the stored tuples
-- are not written, so their triggers do not fire. Function targets get
-- 'reset' first and then 'insert' for each row.
local Load   = {}
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
        stored       = item.space:replace(tuple)
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
  apply(rows, function (row) self:write(row) end)
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

local Position   = {}
Position.__index = Position

local NUMERIC = { unsigned = true, integer = true, number = true }

-- Opens the position space, created when missing with a key of the type of
-- key. key is a stable number or string of this replicator, the replica server
-- id or the slot by default; the tuple is { key, position, event_time, kind },
-- kind the type of the source ('mysql', 'postgresql'), so a position of
-- another kind under the same key is not taken for its own.
local function position(space_name, key, name, kind)
  local space = box.space[space_name]
  if space == nil then
    space = box.schema.space.create(space_name, { if_not_exists = true })
    space:create_index('ID', { if_not_exists = true, unique = true,
      parts = { 1, type(key) == 'number' and 'unsigned' or 'string' } })
  end
  if space.index[0] == nil then
    errors.fatal('position space %s has no primary index', space_name)
  end
  local part = space.index[0].parts[1].type
  if (part == 'string' and type(key) ~= 'string') or (NUMERIC[part] and type(key) ~= 'number') then
    errors.fatal('position space %s has a %s key, the key %s does not fit it', space_name, part, tostring(key))
  end
  return setmetatable({ space = space, key = key, name = name, kind = kind }, Position)
end

-- Returns the stored position or nil; a position of another kind of source
-- is not used (a full load follows), one without the kind (older versions)
-- is taken
function Position:get()
  local tuple = self.space:get(self.key)
  if tuple == nil or type(tuple[2]) ~= 'string' then
    return nil
  end
  if type(tuple[4]) == 'string' and tuple[4] ~= self.kind then
    log.warn('MirrorFlayer[%s]: the position under %s is one of %s, not of %s; a full load follows', self.name,
      tostring(self.key), tuple[4], tostring(self.kind))
    return nil
  end
  return tuple[2]
end

-- Stores the position; call inside the transaction of the applied changes
function Position:put(state, time)
  self.space:replace({ self.key, state, time or 0, self.kind })
end

-- Prepared XA transactions of the source: { key, xid, events } in a space of
-- their own, made when the first one comes; nil without it when not create
local Prepared   = {}
Prepared.__index = Prepared

local function prepared(space_name, key, create)
  local space = box.space[space_name]
  if space == nil then
    if not create then
      return nil
    end
    space = box.schema.space.create(space_name, { if_not_exists = true })
    space:create_index('ID', { if_not_exists = true, unique = true,
      parts = { 1, type(key) == 'number' and 'unsigned' or 'string', 2, 'string' } })
  end
  return setmetatable({ space = space, key = key }, Prepared)
end

function Prepared:put(xid, events)
  self.space:replace({ self.key, xid, events })
end

function Prepared:get(xid)
  local tuple = self.space:get({ self.key, xid })
  return tuple and tuple[3]
end

function Prepared:delete(xid)
  self.space:delete({ self.key, xid })
end

return {
  prepared    = prepared,
  target      = target,
  load        = load,
  transaction = transaction,
  apply       = apply,
  configure   = configure,
  aborts      = function () return pace.aborts end,
  position    = position
}
