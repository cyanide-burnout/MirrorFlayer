-- MirrorFlayer.MySQL: the source driver of MySQL and MariaDB. Tables are read
-- with COM_QUERY, changes come from the binlog (binlog_format=ROW,
-- binlog_row_image=FULL), the position is a GTID state: d-s-n,d-s-n of
-- MariaDB or a GTID set uuid:1-100 of MySQL.
--
-- A driver gives the engine (MirrorFlayer) a session on one host:
--   session:describe(source)          columns of the table
--   session:position()                the current position of the source
--   session:load(source, each)        the rows of the table, values converted
--   session:stream(state, sources, handler)
--                                     changes after the position, until an error:
--                                     handler:started(), handler:commit(changes,
--                                     position, event_time), handler:heartbeat(),
--                                     handler:refresh(source)
--   session:close()
-- A change is { source, operation, old, new }; operation is 'insert',
-- 'update' or 'delete', old and new are rows in the order of source.columns.

local protocol = require('MirrorFlayer.MySQL.Protocol')
local column   = require('MirrorFlayer.MySQL.Column')
local binlog   = require('MirrorFlayer.MySQL.BinLog')
local mariadb  = require('MirrorFlayer.MySQL.MariaDB')
local gtid     = require('MirrorFlayer.MySQL.GTID')
local errors   = require('MirrorFlayer.Failure')

local E = binlog.E

local failure, fatal = errors.failure, errors.fatal

-- The binlog no longer has the position asked for
local LOST_POSITION = 1236

local COMPRESSED = 'compressed binlog events are not supported, turn log_bin_compress off'

-- Events that stop the replicator
local UNSUPPORTED = {
  [E.QUERY_COMPRESSED] = COMPRESSED,
  [E.WRITE_ROWS_COMPRESSED_V1] = COMPRESSED, [E.UPDATE_ROWS_COMPRESSED_V1] = COMPRESSED, [E.DELETE_ROWS_COMPRESSED_V1] = COMPRESSED,
  [E.WRITE_ROWS_COMPRESSED] = COMPRESSED, [E.UPDATE_ROWS_COMPRESSED] = COMPRESSED, [E.DELETE_ROWS_COMPRESSED] = COMPRESSED,
  [E.ANONYMOUS_GTID] = 'transaction without GTID: the source needs gtid_mode=ON',
  [E.GTID_TAGGED] = 'tagged GTIDs are not supported',
  [E.TRANSACTION_PAYLOAD] = 'compressed transactions are not supported, turn binlog_transaction_compression off',
  [E.PARTIAL_UPDATE_ROWS] = 'partial JSON updates are not supported, binlog_row_value_options must be empty',
  [E.XA_PREPARE] = 'XA transactions are not supported'
}

local Session = {}
Session.__index = Session

-- Checks the settings of the driver; returns a message when they are wrong
local function check(config)
  if type(config.server) ~= 'number' or config.server <= 0 then
    return 'config.server must be a positive number'
  end
end

-- The key of the position by default: the replica server id
local function key(config)
  return config.server
end

-- Connects to one host. config: user, password, timeout, tls, public_key,
-- server, heartbeat, datetime, json
local function connect(config, host, port)
  local connection = protocol.connect({
    host = host,
    port = port,
    user = config.user,
    password = config.password,
    timeout = config.timeout,
    tls = config.tls,
    public_key = config.public_key
  })
  local status, problem = pcall(connection.query, connection, "SET time_zone = '+00:00'")
  if not status then
    connection:close()
    error(problem, 0)
  end
  return setmetatable({
    config = config,
    connection = connection,
    version = connection.version,
    secure = connection.secure == true,
    dialect = connection.mariadb and mariadb or gtid
  }, Session)
end

function Session:close()
  self.connection:close()
end

-- Reads the columns of the source table from information_schema
function Session:describe(source)
  local connection = self.connection
  local _, rows = connection:query(string.format(
    'SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS ' ..
    'WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION',
    protocol.literal(source.database), protocol.literal(source.table)))
  if #rows == 0 then
    error(failure(string.format('table %s does not exist or is not visible', source.id)), 0)
  end
  local columns = {}
  for index, row in ipairs(rows) do
    columns[index] = column.describe(row[1], row[2], row[3], self.config)
  end
  if connection.mariadb then
    -- JSON of MariaDB is LONGTEXT with a json_valid() check
    local _, checks = connection:query(string.format(
      'SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = %s AND TABLE_NAME = %s',
      protocol.literal(source.database), protocol.literal(source.table)))
    local json = {}
    for _, item in ipairs(checks) do
      local name = string.match(item[1], '^json_valid%(`(.-)`%)$')
      if name ~= nil then
        json[name] = true
      end
    end
    for _, item in ipairs(columns) do
      if json[item.name] and item.kind == 'string' then
        item.kind = 'json'
      end
    end
  end
  return columns
end

-- The GTID state of the server. The full load takes it first: changes made
-- while the tables are read come again from the binlog, and applying them is
-- idempotent
function Session:position()
  return self.dialect.position(self.connection)
end

-- Reads the table row by row, values converted, for each(row)
function Session:load(source, each)
  local columns = source.columns
  local names   = {}
  for index, item in ipairs(columns) do
    names[index] = protocol.identifier(item.name)
  end
  self.connection:query(string.format('SELECT %s FROM %s.%s', table.concat(names, ', '),
    protocol.identifier(source.database), protocol.identifier(source.table)), function (raw)
      local row = {}
      for index, item in ipairs(columns) do
        row[index] = column.text(item, raw[index])
      end
      each(row)
    end)
end

-- Binlog stream --------------------------------------------------------------

local function statement(text)
  return string.upper(string.match(text, '^%s*(.-)%s*;?%s*$'))
end

function Session:truncates(text)
  local upper = statement(text)
  if string.match(upper, '^TRUNCATE') == nil then
    return false
  end
  for _, source in pairs(self.sources) do
    if string.find(upper, string.upper(source.table), 1, true) ~= nil then
      return true
    end
  end
  return false
end

-- Hands a finished transaction to the engine with its position
function Session:commit(header)
  local transaction = self.transaction
  self.transaction = nil
  if transaction == nil then
    return
  end
  self.dialect.add(self.state, transaction)
  self.handler:commit(transaction.changes, self.dialect.format(self.state), header.timestamp)
end

function Session:event(packet)
  local header = binlog.event(packet, self.checksum)
  local type   = header.type

  if type == E.FORMAT_DESCRIPTION then
    self.checksum = header.checksum == 1
  elseif type == E.HEARTBEAT or type == E.HEARTBEAT_V2 then
    self.handler:heartbeat()
  elseif type == E.GTID or type == E.GTID_MYSQL then
    local transaction = self.dialect.gtid(header)
    if self.transaction ~= nil then
      self.handler:say('warn', 'transaction %s has no end, dropped', self.transaction.gtid)
    end
    transaction.changes = {}
    self.transaction = transaction
  elseif type == E.TABLE_MAP then
    local map    = binlog.table_map(header)
    local source = self.sources[map.schema .. '.' .. map.table]
    if source == nil then
      self.maps[map.id] = false
      return
    end
    if source.stale or #map.types ~= #source.columns then
      self.handler:refresh(source)
    end
    if #map.types ~= #source.columns then
      -- The binlog predates the current table definition
      errors.reload('%s: binlog has %d columns, table has %d; reloading', source.id, #map.types, #source.columns)
    end
    map.source = source
    self.maps[map.id] = map
  elseif binlog.ROWS[type] ~= nil then
    local map = self.maps[binlog.rows_table(header)]
    if map == nil then
      error(failure('rows event without a table map'), 0)
    end
    if map == false then
      return
    end
    if self.transaction == nil then
      error(failure('rows event outside of a transaction'), 0)
    end
    local operation, rows = binlog.rows(header, map, map.source.columns)
    local changes = self.transaction.changes
    for _, row in ipairs(rows) do
      table.insert(changes, { map.source, operation, row[1], row[2] })
    end
  elseif type == E.XID then
    self:commit(header)
  elseif type == E.QUERY then
    local query = binlog.query(header)
    local text  = statement(query.query)
    if text == 'BEGIN' then
      if self.transaction ~= nil then
        self.transaction.standalone = false
      end
      return
    end
    if text == 'COMMIT' then
      self:commit(header)
      return
    end
    if text == 'ROLLBACK' then
      if self.transaction ~= nil then
        self.transaction.changes = {}
      end
      self:commit(header)
      return
    end
    if self.transaction ~= nil and self.transaction.standalone then
      -- DDL and other statements outside transactions
      for _, source in pairs(self.sources) do
        source.stale = true
      end
      if self:truncates(query.query) then
        errors.reload('mapped table truncated, reloading')
      end
      self:commit(header)
    end
  elseif UNSUPPORTED[type] ~= nil then
    fatal('%s', UNSUPPORTED[type])
  end
  -- ROTATE, STOP, GTID_LIST, BINLOG_CHECKPOINT, ANNOTATE_ROWS, ROWS_QUERY,
  -- PREVIOUS_GTIDS, TRANSACTION_CONTEXT, VIEW_CHANGE need nothing
end

-- Streams the binlog after the GTID state. sources maps 'database.table' to
-- the mapped sources; handler gets the transactions. Returns only by an error;
-- a position the binlog no longer has asks for a full load
function Session:stream(state, sources, handler)
  local connection = self.connection
  self.sources     = sources
  self.handler     = handler
  self.state       = self.dialect.parse(state)
  self.maps        = {}
  self.transaction = nil
  self.checksum    = self.dialect.request(connection, {
    state = state,
    server = self.config.server,
    heartbeat = self.config.heartbeat
  })
  handler:started()

  local timeout = self.config.heartbeat * 3
  while true do
    local packet = connection:read_packet(timeout)
    local first  = string.byte(packet, 1)
    if first == 0xff then
      local problem = protocol.server_error(packet)
      if problem.code == LOST_POSITION then
        problem.reload = true
      end
      error(problem, 0)
    end
    if first == 0xfe and #packet < 9 then
      error(failure('binlog stream ended by the server'), 0)
    end
    if first ~= 0x00 then
      error(failure(string.format('unexpected packet 0x%02x in the binlog stream', first)), 0)
    end
    self:event(string.sub(packet, 2))
  end
end

return {
  PORT = 3306,
  check = check,
  key = key,
  connect = connect
}
