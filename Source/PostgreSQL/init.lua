-- MirrorFlayer.PostgreSQL: the source driver of PostgreSQL over logical
-- replication with pgoutput. The position is the slot and the LSN, 'slot/X/Y'.
--
-- The full load creates the replication slot anew with an exported snapshot
-- and reads the tables in that snapshot over a second connection, so the
-- load and the stream meet exactly. The stream confirms the LSN of every
-- applied transaction to the server, which then frees the WAL before it.
--
-- The server needs wal_level=logical, a publication of the mapped tables and
-- a role with REPLICATION (or a superuser). UPDATE and DELETE send the old
-- key of the REPLICA IDENTITY (the primary key by default): the primary key
-- of a target space has to be among its columns, or the table needs REPLICA
-- IDENTITY FULL. The slot keeps WAL while the replicator is away; drop it
-- with pg_drop_replication_slot() when the replicator is gone for good.
--
-- map(schema, table, ...) names the schema and the table as they are in the
-- catalogue (case-sensitive), the database is config.database.

local clock = require('clock')

local protocol = require('MirrorFlayer.PostgreSQL.Protocol')
local column   = require('MirrorFlayer.PostgreSQL.Column')
local output   = require('MirrorFlayer.PostgreSQL.Output')
local errors   = require('MirrorFlayer.Failure')
local convert  = require('MirrorFlayer.Column')

local failure = errors.failure

local NULL      = box.NULL
local UNCHANGED = convert.UNCHANGED

local SLOT        = 'mirrorflayer'
local PUBLICATION = 'mirrorflayer'

-- SQLSTATE of an object that does not exist (a slot among others)
local UNDEFINED_OBJECT = '42704'

local Session = {}
Session.__index = Session

local function slot(config)
  return config.slot or SLOT
end

-- Checks the settings of the driver; returns a message when they are wrong
local function check(config)
  local name = slot(config)
  if type(name) ~= 'string' or string.match(name, '^[a-z0-9_]+$') == nil or #name > 63 then
    return 'config.slot must be a name of lowercase letters, digits and underscores, at most 63'
  end
  if config.publication ~= nil and (type(config.publication) ~= 'string' or config.publication == '') then
    return 'config.publication must be a name'
  end
  if config.database ~= nil and type(config.database) ~= 'string' then
    return 'config.database must be a string'
  end
end

-- The key of the position by default: the name of the slot
local function key(config)
  return slot(config)
end

local function open(config, host, port, replication)
  return protocol.connect({
    host = host,
    port = port,
    user = config.user,
    password = config.password,
    database = config.database,
    timeout = config.timeout,
    tls = config.tls,
    replication = replication
  })
end

-- Connects to one host. config: user, password, database, slot, publication,
-- timeout, tls, heartbeat, datetime, json, uuid
local function connect(config, host, port)
  local connection = open(config, host, port, true)
  return setmetatable({
    config = config,
    host = host,
    port = port,
    connection = connection,
    version = connection.version,
    secure = connection.secure == true,
    slot = slot(config),
    publication = config.publication or PUBLICATION
  }, Session)
end

function Session:close()
  if self.loader ~= nil then
    self.loader:close()
    self.loader = nil
  end
  self.connection:close()
end

-- Reads the columns of the table from the catalogue: without dropped and
-- generated columns, in the order pgoutput sends them
function Session:describe(source)
  local _, rows = self.connection:query(string.format([[
SELECT a.attname, a.atttypid, b.oid, b.typname, b.typtype, b.typcategory
  FROM pg_catalog.pg_attribute a
  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
  JOIN pg_catalog.pg_type b ON b.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END
 WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
 ORDER BY a.attnum]], protocol.literal(source.database), protocol.literal(source.table)))
  if #rows == 0 then
    error(failure(string.format('table %s does not exist or is not visible', source.id)), 0)
  end
  local columns = {}
  for index, row in ipairs(rows) do
    columns[index] = column.describe(row[1], row[2], row[3], row[4], row[5], row[6], self.config)
  end
  return columns
end

-- Creates the slot anew with an exported snapshot and opens a transaction in
-- that snapshot over a second connection for load(); the position is where
-- the slot begins. The replication connection must stay idle until stream()
function Session:position()
  local connection = self.connection
  local status, problem = pcall(connection.query, connection,
    'DROP_REPLICATION_SLOT ' .. protocol.identifier(self.slot) .. ' WAIT')
  if not status and not (type(problem) == 'table' and problem.code == UNDEFINED_OBJECT) then
    error(problem, 0)
  end
  local _, rows = connection:query('CREATE_REPLICATION_SLOT ' .. protocol.identifier(self.slot) ..
    ' LOGICAL pgoutput EXPORT_SNAPSHOT')
  local start, snapshot = rows[1][2], rows[1][3]
  self.loader = open(self.config, self.host, self.port, false)
  self.loader:query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
  self.loader:query('SET TRANSACTION SNAPSHOT ' .. protocol.literal(snapshot))
  return self.slot .. '/' .. start
end

-- Reads the table row by row, values converted: in the snapshot of the slot
-- after position(), else in a snapshot of its own (a table loaded alone, the
-- stream from the stored position converges over it)
function Session:load(source, each)
  if self.loader == nil then
    self.loader = open(self.config, self.host, self.port, false)
    self.loader:query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
  end
  local columns = source.columns
  local names   = {}
  for index, item in ipairs(columns) do
    names[index] = protocol.identifier(item.name)
  end
  self.loader:query(string.format('SELECT %s FROM %s.%s', table.concat(names, ', '),
    protocol.identifier(source.database), protocol.identifier(source.table)), function (raw)
      local row = {}
      for index, item in ipairs(columns) do
        row[index] = column.text(item, raw[index])
      end
      each(row)
    end)
end

-- Stream -------------------------------------------------------------------------

-- Standby status update: the LSN applied, written and flushed is the one
-- confirmed; reply asks the server for a keepalive at once
function Session:feedback(reply)
  local now = clock.time()
  self.connection:send('d', 'r' .. protocol.u64(self.confirmed) .. protocol.u64(self.confirmed) ..
    protocol.u64(self.confirmed) .. protocol.i64(math.floor((now - output.EPOCH) * 1e6)) .. (reply and '\1' or '\0'))
  self.reported = now
end

-- Binds a relation to its source: positions of its columns among the
-- columns of the source. The table is described again when they differ; a
-- relation that still differs predates the table, which asks for a full load
function Session:relation(message)
  local source = self.sources[message.namespace .. '.' .. message.name]
  self.relations[message.relation] = message
  if source == nil then
    return
  end
  local function bind()
    local positions = {}
    for index, item in ipairs(source.columns) do
      positions[item.name] = index
    end
    local result = {}
    for index, item in ipairs(message.columns) do
      local position = positions[item.name]
      if position == nil or source.columns[position].oid ~= item.oid then
        return nil
      end
      result[index] = position
    end
    if #message.columns ~= #source.columns then
      return nil
    end
    return result
  end
  local positions = bind()
  if positions == nil then
    self.handler:refresh(source)
    positions = bind()
  end
  if positions == nil then
    errors.reload('%s: the stream has other columns than the table; reloading', source.id)
  end
  message.source    = source
  message.positions = positions
end

-- A row of the source from a tuple of the relation
local function row(relation, values)
  if values == nil then
    return nil
  end
  local columns = relation.source.columns
  local result  = {}
  for index = 1, #columns do
    result[index] = NULL
  end
  for index, value in ipairs(values) do
    local position = relation.positions[index]
    if value == NULL or rawequal(value, UNCHANGED) then
      result[position] = value
    else
      result[position] = column.text(columns[position], value)
    end
  end
  return result
end

function Session:change(message)
  local relation = self.relations[message.relation]
  if relation == nil then
    errors.fatal('pgoutput: change of relation %d before its description', message.relation)
  end
  if relation.source == nil then
    return
  end
  if self.transaction == nil then
    error(failure('pgoutput: change outside of a transaction'), 0)
  end
  local changes = self.transaction.changes
  if message.kind == 'I' then
    table.insert(changes, { relation.source, 'insert', nil, row(relation, message.new) })
  elseif message.kind == 'U' then
    local new = row(relation, message.new)
    local old = row(relation, message.old)
    if old == nil then
      -- The key did not change, the new row has it
      old = new
    elseif message.full then
      for index, value in ipairs(new) do
        if rawequal(value, UNCHANGED) then
          new[index] = old[index]
        end
      end
    end
    table.insert(changes, { relation.source, 'update', old, new })
  else
    table.insert(changes, { relation.source, 'delete', row(relation, message.old), nil })
  end
end

function Session:message(payload)
  local message = output.decode(payload)
  local kind    = message.kind
  if kind == 'B' then
    self.transaction = { changes = {} }
  elseif kind == 'C' then
    local transaction = self.transaction
    self.transaction = nil
    if transaction == nil then
      return
    end
    self.handler:commit(transaction.changes, self.slot .. '/' .. output.format(message.finish), message.time)
    self.confirmed = message.finish
    if clock.time() - self.reported >= 1 then
      self:feedback(false)
    end
  elseif kind == 'R' then
    self:relation(message)
  elseif kind == 'I' or kind == 'U' or kind == 'D' then
    self:change(message)
  elseif kind == 'T' then
    for _, id in ipairs(message.relations) do
      local relation = self.relations[id]
      if relation ~= nil and relation.source ~= nil then
        errors.reload('%s truncated, reloading', relation.source.id)
      end
    end
  end
end

-- A keepalive: everything before its LSN has been sent, so outside of a
-- transaction the position moves there
function Session:keepalive(payload)
  local data  = protocol.reader(payload)
  local stop  = data:u64()
  data:i64()
  local reply = data:u8() == 1
  if self.transaction == nil and stop > self.confirmed then
    self.confirmed = stop
    self.handler:advance(self.slot .. '/' .. output.format(stop))
  end
  self.handler:heartbeat()
  if reply or clock.time() - self.reported >= 1 then
    self:feedback(false)
  end
end

-- Streams the changes after the position. sources maps 'schema.table' to
-- the mapped sources; handler gets the transactions. Returns only by an
-- error; a slot that does not exist asks for a full load
function Session:stream(state, sources, handler)
  local connection = self.connection
  if self.loader ~= nil then
    self.loader:query('COMMIT')
    self.loader:close()
    self.loader = nil
  end
  local name, start = string.match(state, '^([^/]+)/(%x+/%x+)$')
  if name == nil then
    errors.reload('position %q is not one of PostgreSQL; reloading', state)
  end
  if name ~= self.slot then
    errors.reload('the position belongs to slot %s, not %s; reloading', name, self.slot)
  end
  self.sources     = sources
  self.handler     = handler
  self.relations   = {}
  self.transaction = nil
  self.confirmed   = output.lsn(start)
  self.reported    = 0

  local status, problem, _, _, copy = pcall(connection.query, connection, string.format(
    "START_REPLICATION SLOT %s LOGICAL %s (proto_version '1', publication_names %s)",
    protocol.identifier(self.slot), start, protocol.literal(protocol.identifier(self.publication))))
  if not status then
    if type(problem) == 'table' and problem.code == UNDEFINED_OBJECT and
       string.find(problem.message, 'replication slot', 1, true) ~= nil then
      problem.reload = true
    end
    error(problem, 0)
  end
  if not copy then
    error(failure('START_REPLICATION did not start a copy'), 0)
  end
  handler:started()

  local heartbeat = self.config.heartbeat
  local received  = clock.time()
  while true do
    if connection:wait(heartbeat) then
      local kind, payload = connection:receive()
      received = clock.time()
      if kind == 'd' then
        local tag = string.sub(payload, 1, 1)
        if tag == 'w' then
          -- XLogData: start, end of WAL, send time, then the message
          self:message(string.sub(payload, 26))
        elseif tag == 'k' then
          self:keepalive(string.sub(payload, 2))
        end
      elseif kind == 'E' then
        local problem = protocol.server_error(payload)
        if problem.code == UNDEFINED_OBJECT and string.find(problem.message, 'replication slot', 1, true) ~= nil then
          problem.reload = true
        end
        error(problem, 0)
      elseif kind == 'c' then
        error(failure('replication stream ended by the server'), 0)
      end
      -- NoticeResponse, ParameterStatus
    else
      if clock.time() - received > heartbeat * 3 then
        error(failure(string.format('no data from the server for %d s', heartbeat * 3)), 0)
      end
      -- Asks for a keepalive, which tells that the server is there
      self:feedback(true)
    end
  end
end

return {
  PORT = 5432,
  check = check,
  key = key,
  connect = connect
}
