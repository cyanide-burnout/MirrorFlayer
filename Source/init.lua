-- MirrorFlayer: replication of MySQL/MariaDB tables into Tarantool spaces
-- over the binlog replication protocol, in a fiber of Tarantool itself
--
--   local MirrorFlayer = require('MirrorFlayer')
--   local replica = MirrorFlayer.new({
--     hosts = { 'db1:3306', 'db2:3306' }, user = 'replica', password = '...',
--     server = 1001
--   })
--   replica:map('Registry', 'Locations', 'GlobalLocations', { 'ID', 'Node', 'Kind', 'Link', 'Slot', 'Modified' })
--   replica:start()

local fiber = require('fiber')
local clock = require('clock')
local log   = require('log')

local protocol = require('MirrorFlayer.Protocol')
local column   = require('MirrorFlayer.Column')
local binlog   = require('MirrorFlayer.BinLog')
local mariadb  = require('MirrorFlayer.MariaDB')
local mysql    = require('MirrorFlayer.MySQL')
local store    = require('MirrorFlayer.Store')

local E = binlog.E

local failure, fatal = protocol.failure, protocol.fatal

local DEFAULTS = {
  name = 'MirrorFlayer',
  position = 'ReplicatorPositions',
  tls = false,
  heartbeat = 10,
  timeout = 30,
  batch = 100,
  retry = 5
}

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

local Replica = {}
Replica.__index = Replica

local function address(text)
  local host, port = string.match(text, '^%[(.+)%]:(%d+)$')
  if host == nil then
    host, port = string.match(text, '^([^:]+):(%d+)$')
  end
  if host == nil then
    host = text
  end
  return host, tonumber(port) or 3306
end

local function check(condition, format, ...)
  if not condition then
    error(string.format(format, ...), 3)
  end
end

-- Creates a replicator.
-- config: hosts (array of 'host[:port]'), user, password, server (replica
-- server id), tls (false, true or { verify = 'none' | 'ca' | 'identity', ca,
-- cert, key }), public_key (PEM of the source for caching_sha2_password
-- without TLS, asked from the server when missing), position (space name), key (of the position, the
-- server id by default), name (in logs and metrics), heartbeat (seconds),
-- timeout (seconds), batch (rows per transaction of the full load), retry
-- (seconds between connection attempts)
local function new(config)
  check(type(config) == 'table', 'config must be a table')
  check(type(config.hosts) == 'table' and #config.hosts > 0, 'config.hosts must be a non-empty array')
  check(type(config.user) == 'string', 'config.user must be a string')
  check(config.password == nil or type(config.password) == 'string', 'config.password must be a string')
  check(type(config.server) == 'number' and config.server > 0, 'config.server must be a positive number')
  local options = {}
  for key, value in pairs(DEFAULTS) do
    options[key] = value
  end
  for key, value in pairs(config) do
    options[key] = value
  end
  check(type(options.position) == 'string', 'config.position must be a space name')
  check(type(options.name) == 'string', 'config.name must be a string')
  if options.tls == true then
    options.tls = { verify = 'none' }
  elseif type(options.tls) == 'table' then
    local tls = {}
    for key, value in pairs(options.tls) do
      tls[key] = value
    end
    tls.verify = tls.verify or (tls.ca ~= nil and 'ca' or 'none')
    check(tls.verify == 'none' or tls.verify == 'ca' or tls.verify == 'identity',
      "config.tls.verify must be 'none', 'ca' or 'identity'")
    options.tls = tls
  else
    check(not options.tls, 'config.tls must be a boolean or a table')
    options.tls = nil
  end
  check(options.public_key == nil or type(options.public_key) == 'string', 'config.public_key must be a PEM string')
  if options.key == nil then
    options.key = options.server
  end
  check((type(options.key) == 'number' and options.key >= 0 and options.key == math.floor(options.key)) or
    type(options.key) == 'string', 'config.key must be an unsigned integer or a string')
  return setmetatable({
    config = options,
    sources = {},
    order = {},
    status = 'created',
    stats = { transactions = 0, rows = 0, resyncs = 0, split = 0 }
  }, Replica)
end

-- Maps a source table to a target: a space name or function (operation, old,
-- new) called inside the transaction ('insert' | 'update' | 'delete', and
-- 'reset' before a full reload). columns are source column names in tuple
-- order; by default the field names of the space format.
function Replica:map(database, table_name, target, columns)
  check(self.fiber == nil, 'map() must be called before start()')
  check(type(database) == 'string' and type(table_name) == 'string', 'database and table must be strings')
  check(type(target) == 'string' or type(target) == 'function', 'target must be a space name or function')
  check(columns == nil or type(columns) == 'table', 'columns must be an array of names')
  local id     = database .. '.' .. table_name
  local source = self.sources[id]
  if source == nil then
    source = { id = id, database = database, table = table_name, specs = {} }
    self.sources[id] = source
    table.insert(self.order, source)
  end
  table.insert(source.specs, { target = target, columns = columns })
  return self
end

function Replica:say(level, format, ...)
  log[level]('MirrorFlayer[%s]: ' .. format, self.config.name, ...)
end

-- Connections ----------------------------------------------------------------

function Replica:connect(host, port)
  local connection = protocol.connect({
    host = host,
    port = port,
    user = self.config.user,
    password = self.config.password,
    timeout = self.config.timeout,
    tls = self.config.tls,
    public_key = self.config.public_key
  })
  local status, problem = pcall(connection.query, connection, "SET time_zone = '+00:00'")
  if not status then
    connection:close()
    error(problem, 0)
  end
  return connection
end

-- Reads the columns of the source table and binds the targets to them
local function describe(connection, source)
  local _, rows = connection:query(string.format(
    'SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS ' ..
    'WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION',
    protocol.literal(source.database), protocol.literal(source.table)))
  if #rows == 0 then
    error(failure(string.format('table %s does not exist or is not visible', source.id)), 0)
  end
  local columns = {}
  for index, row in ipairs(rows) do
    columns[index] = column.describe(row[1], row[2], row[3])
  end
  for _, target in ipairs(source.targets) do
    target:bind(columns)
  end
  source.columns = columns
  source.stale = false
end

function Replica:refresh(source)
  local connection = self:connect(self.host, self.port)
  local status, problem = pcall(describe, connection, source)
  connection:close()
  if not status then
    error(problem, 0)
  end
end

-- Full load ------------------------------------------------------------------

-- Remembers the binlog position first, then loads every table; changes made
-- meanwhile come again from the binlog and applying them is idempotent
function Replica:resync(connection)
  local state   = self.dialect.position(connection)
  local started = clock.time()
  self.status = 'loading'
  self:say('info', 'full load at GTID %s', state)
  for _, source in ipairs(self.order) do
    local loader = store.load(source.targets, self.config.batch)
    local aborts = store.aborts()
    local names  = {}
    for index, item in ipairs(source.columns) do
      names[index] = protocol.identifier(item.name)
    end
    local columns = source.columns
    local count   = 0
    connection:query(string.format('SELECT %s FROM %s.%s', table.concat(names, ', '),
      protocol.identifier(source.database), protocol.identifier(source.table)), function (raw)
        local row = {}
        for index, item in ipairs(columns) do
          row[index] = column.text(item, raw[index])
        end
        loader:add(row)
        count = count + 1
      end)
    local deleted = loader:finish()
    self:say('info', '%s: %d rows loaded, %d written, %d stale rows deleted, %d aborted by triggers', source.id,
      count, loader.written, deleted, store.aborts() - aborts)
  end
  store.transaction(function ()
    self.position:put(state, clock.time())
  end)
  self.stats.resyncs = self.stats.resyncs + 1
  self.stats.position = state
  self.resync_needed = false
  self:say('info', 'full load done in %.1f s', clock.time() - started)
  return state
end

-- Binlog stream --------------------------------------------------------------

function Replica:flush()
  if not self.dirty then
    return
  end
  store.transaction(function ()
    self.position:put(self.stats.position, self.stats.event_time or 0)
  end)
  self.dirty = false
  self.stored = clock.time()
end

-- Applies a finished binlog transaction with its position in one box
-- transaction; when a trigger of a target yields, statement by statement and
-- the position last (counted in split). Transactions without changes only
-- move the position, which is written at most once a second
function Replica:commit(header)
  local transaction = self.transaction
  self.transaction = nil
  if transaction == nil then
    return
  end
  self.dialect.add(self.state, transaction)
  local state = self.dialect.format(self.state)
  local stats = self.stats
  stats.position = state
  stats.event_time = header.timestamp
  if #transaction.changes == 0 then
    self.dirty = true
    if clock.time() - (self.stored or 0) >= 1 then
      self:flush()
    end
    return
  end
  local statements = {}
  for _, change in ipairs(transaction.changes) do
    for _, target in ipairs(change[1].targets) do
      table.insert(statements, { target, change[2], change[3], change[4] })
    end
  end
  local atomic = store.apply(statements, function (item)
    item[1]:apply(item[2], item[3], item[4])
  end, function ()
    self.position:put(state, header.timestamp)
  end)
  if not atomic then
    stats.split = stats.split + 1
  end
  self.dirty = false
  self.stored = clock.time()
  stats.applied_time = self.stored
  stats.lag = math.max(0, self.stored - header.timestamp)
  stats.transactions = stats.transactions + 1
  stats.rows = stats.rows + #transaction.changes
end

local function statement(text)
  return string.upper(string.match(text, '^%s*(.-)%s*;?%s*$'))
end

function Replica:truncates(text)
  local upper = statement(text)
  if string.match(upper, '^TRUNCATE') == nil then
    return false
  end
  for _, source in ipairs(self.order) do
    if string.find(upper, string.upper(source.table), 1, true) ~= nil then
      return true
    end
  end
  return false
end

function Replica:event(packet)
  local header = binlog.event(packet, self.checksum)
  local type   = header.type

  if type == E.FORMAT_DESCRIPTION then
    self.checksum = header.checksum == 1
  elseif type == E.HEARTBEAT or type == E.HEARTBEAT_V2 then
    self.stats.heartbeat_time = clock.time()
    self.stats.lag = 0
    self:flush()
  elseif type == E.GTID or type == E.GTID_MYSQL then
    local gtid = self.dialect.gtid(header)
    if self.transaction ~= nil then
      self:say('warn', 'transaction %s has no end, dropped', self.transaction.gtid)
    end
    gtid.changes = {}
    self.transaction = gtid
  elseif type == E.TABLE_MAP then
    local map    = binlog.table_map(header)
    local source = self.sources[map.schema .. '.' .. map.table]
    if source == nil then
      self.maps[map.id] = false
      return
    end
    if source.stale or #map.types ~= #source.columns then
      self:refresh(source)
    end
    if #map.types ~= #source.columns then
      -- The binlog predates the current table definition
      self.resync_needed = true
      error(failure(string.format('%s: binlog has %d columns, table has %d; reloading',
        source.id, #map.types, #source.columns)), 0)
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
      for _, source in ipairs(self.order) do
        source.stale = true
      end
      if self:truncates(query.query) then
        self.resync_needed = true
        error(failure('mapped table truncated, reloading'), 0)
      end
      self:commit(header)
    end
  elseif UNSUPPORTED[type] ~= nil then
    fatal('%s', UNSUPPORTED[type])
  end
  -- ROTATE, STOP, GTID_LIST, BINLOG_CHECKPOINT, ANNOTATE_ROWS, ROWS_QUERY,
  -- PREVIOUS_GTIDS, TRANSACTION_CONTEXT, VIEW_CHANGE need nothing
end

function Replica:session(host, port)
  local connection = self:connect(host, port)
  self.connection = connection
  self.host, self.port = host, port
  self.dialect   = connection.mariadb and mariadb or mysql
  self.stats.tls = connection.secure == true
  self:say('info', 'connected to %s:%d, %s%s', host, port, connection.version, connection.secure and ', TLS' or '')

  for _, source in ipairs(self.order) do
    describe(connection, source)
  end

  local state = self.position:get()
  if state == nil or self.resync_needed then
    state = self:resync(connection)
  end
  self.state = self.dialect.parse(state)
  self.stats.position = state
  self.maps = {}
  self.transaction = nil
  self.dirty = false

  self.checksum = self.dialect.request(connection, {
    state = state,
    server = self.config.server,
    heartbeat = self.config.heartbeat
  })
  self.status = 'streaming'
  self.stats.error = nil
  self:say('info', 'streaming from GTID %s', state ~= '' and state or '(start)')

  local timeout = self.config.heartbeat * 3
  while true do
    local packet = connection:read_packet(timeout)
    local first  = string.byte(packet, 1)
    if first == 0xff then
      error(protocol.server_error(packet), 0)
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

-- Keeps failure objects, adds the stack to unexpected errors
local function trace(problem)
  if type(problem) == 'table' then
    return problem
  end
  return debug.traceback(tostring(problem), 2)
end

local function loop(self)
  local hosts = self.config.hosts
  local index = 1
  while not self.stopping do
    local host, port = address(hosts[index])
    local status, problem = xpcall(self.session, trace, self, host, port)
    if self.connection ~= nil then
      self.connection:close()
      self.connection = nil
    end
    if box.is_in_txn() then
      box.rollback()
    end
    if self.stopping then
      break
    end
    self.stats.error = tostring(problem)
    if type(problem) == 'table' and problem.fatal then
      self.status = 'failed'
      self:say('error', 'stopped: %s', tostring(problem))
      break
    end
    if type(problem) == 'table' and problem.code == 1236 then
      self.resync_needed = true
      self:say('warn', '%s:%d: position is not in the binlog, full reload: %s', host, port, tostring(problem))
    else
      self:say('error', '%s:%d: %s', host, port, tostring(problem))
    end
    index = index % #hosts + 1
    self.status = 'waiting'
    if not pcall(fiber.sleep, self.config.retry) then
      break
    end
  end
  if self.status ~= 'failed' then
    self.status = 'stopped'
  end
end

-- Metrics ----------------------------------------------------------------------

function Replica:collect()
  local stat   = self:stat()
  local labels = { name = self.config.name }
  self.gauges.lag:set(stat.lag or 0, labels)
  self.gauges.event:set(stat.event_time or 0, labels)
  self.gauges.applied:set(stat.applied_time or 0, labels)
end

function Replica:register()
  local status, metrics = pcall(require, 'metrics')
  if not status then
    return
  end
  self.gauges = {
    lag = metrics.gauge('mirrorflayer_lag_seconds', 'Delay of the last applied binlog transaction'),
    event = metrics.gauge('mirrorflayer_event_timestamp_seconds', 'Source time of the last applied binlog event'),
    applied = metrics.gauge('mirrorflayer_applied_timestamp_seconds', 'Local time of the last applied binlog transaction')
  }
  self.callback = function () self:collect() end
  metrics.register_callback(self.callback)
end

function Replica:unregister()
  if self.callback ~= nil then
    require('metrics').unregister_callback(self.callback)
    self.callback = nil
  end
end

-- Control ----------------------------------------------------------------------

function Replica:start()
  check(self.fiber == nil, 'already started')
  check(type(box.cfg) ~= 'function', 'call box.cfg{} before start()')
  check(#self.order > 0, 'nothing is mapped')
  local status, problem = pcall(function ()
    for _, source in ipairs(self.order) do
      source.targets = {}
      for _, spec in ipairs(source.specs) do
        table.insert(source.targets, store.target(spec.target, spec.columns))
      end
    end
    self.position = store.position(self.config.position, self.config.key, self.config.name)
  end)
  if not status then
    error(tostring(problem), 2)
  end
  self.stopping = false
  self.status = 'starting'
  store.configure(self.config.batch)
  self:register()
  self.fiber = fiber.new(loop, self)
  self.fiber:name('MirrorFlayer:' .. self.config.name, { truncate = true })
  self.fiber:set_joinable(true)
  return self
end

function Replica:stop()
  if self.fiber == nil then
    return
  end
  self.stopping = true
  if self.fiber:status() ~= 'dead' then
    self.fiber:cancel()
  end
  pcall(self.fiber.join, self.fiber)
  self.fiber = nil
  self:unregister()
  self.status = 'stopped'
end

-- Returns the state: status, host, position (GTID state), event_time (source
-- time of the last applied transaction), applied_time, lag (seconds between
-- the two, 0 after a heartbeat), counters and the last error
function Replica:stat()
  local stats = self.stats
  return {
    name = self.config.name,
    status = self.status,
    host = self.host and string.format('%s:%d', self.host, self.port) or nil,
    position = stats.position,
    event_time = stats.event_time,
    applied_time = stats.applied_time,
    heartbeat_time = stats.heartbeat_time,
    lag = stats.lag,
    transactions = stats.transactions,
    rows = stats.rows,
    resyncs = stats.resyncs,
    tls = stats.tls,
    split = stats.split,
    aborts = store.aborts(),
    error = stats.error
  }
end

return {
  new = new,
  _VERSION = '0.1.0'
}
