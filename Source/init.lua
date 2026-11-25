-- MirrorFlayer: replication of source tables into Tarantool spaces, in a
-- fiber of Tarantool itself. The source is a driver: MirrorFlayer.MySQL
-- (MySQL and MariaDB over the binlog). The engine keeps the fiber, hosts,
-- retries, transactions, positions, statistics and metrics.
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

local store = require('MirrorFlayer.Store')

-- Source drivers by config.type
local DRIVERS = {
  mysql = 'MirrorFlayer.MySQL'
}

local DEFAULTS = {
  type = 'mysql',
  name = 'MirrorFlayer',
  position = 'ReplicatorPositions',
  tls = false,
  datetime = 'datetime',
  json = 'map',
  heartbeat = 10,
  timeout = 30,
  batch = 100,
  retry = 5
}

local Replica = {}
Replica.__index = Replica

local function address(text, default)
  local host, port = string.match(text, '^%[(.+)%]:(%d+)$')
  if host == nil then
    host, port = string.match(text, '^([^:]+):(%d+)$')
  end
  if host == nil then
    host = text
  end
  return host, tonumber(port) or default
end

local function check(condition, format, ...)
  if not condition then
    error(string.format(format, ...), 3)
  end
end

-- Creates a replicator; the connection settings are checked by start().
-- config: type ('mysql', the default), hosts (array of 'host[:port]'), user,
-- password, server (replica server id of MySQL and MariaDB), tls (false, true
-- or { verify = 'none' | 'ca' | 'identity', ca, cert, key }), public_key (PEM
-- of the source for caching_sha2_password without TLS, asked from the server
-- when missing), position (space name), key (of the position, the server id
-- of MySQL by default), name (in logs and metrics), datetime ('datetime' or
-- 'number'), json ('map' or 'string'), heartbeat (seconds), timeout
-- (seconds), batch (rows per transaction), retry (seconds between connection
-- attempts)
local function new(config)
  check(type(config) == 'table', 'config must be a table')
  local options = {}
  for key, value in pairs(DEFAULTS) do
    options[key] = value
  end
  for key, value in pairs(config) do
    options[key] = value
  end
  check(DRIVERS[options.type] ~= nil, "config.type must be 'mysql'")
  local driver = require(DRIVERS[options.type])
  check(type(options.position) == 'string', 'config.position must be a space name')
  check(type(options.name) == 'string', 'config.name must be a string')
  check(options.datetime == 'datetime' or options.datetime == 'number', "config.datetime must be 'datetime' or 'number'")
  check(options.json == 'map' or options.json == 'string', "config.json must be 'map' or 'string'")
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
    options.key = driver.key(options)
  end
  check((type(options.key) == 'number' and options.key >= 0 and options.key == math.floor(options.key)) or
    type(options.key) == 'string', 'config.key must be an unsigned integer or a string')
  return setmetatable({
    config = options,
    driver = driver,
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

-- Sources ----------------------------------------------------------------------

-- Reads the columns of the source table and binds the targets to them
function Replica:describe(connection, source)
  local columns = connection:describe(source)
  for _, target in ipairs(source.targets) do
    target:bind(columns)
  end
  source.columns = columns
  source.stale = false
end

-- Describes a table again over a connection of its own while the stream
-- holds the current one (the driver asks for it after DDL)
function Replica:refresh(source)
  local connection = self.driver.connect(self.config, self.host, self.port)
  local status, problem = pcall(self.describe, self, connection, source)
  connection:close()
  if not status then
    error(problem, 0)
  end
end

-- Full load ------------------------------------------------------------------

-- Takes the position of the source first, then loads every table; changes
-- made meanwhile come again from the stream and applying them is idempotent
function Replica:resync(connection)
  local state   = connection:position()
  local started = clock.time()
  self.status = 'loading'
  self:say('info', 'full load at %s', state ~= '' and state or '(start)')
  for _, source in ipairs(self.order) do
    local loader = store.load(source.targets, self.config.batch)
    local aborts = store.aborts()
    local count  = 0
    connection:load(source, function (row)
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

-- Stream -------------------------------------------------------------------------

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

-- The driver has asked the source for the stream
function Replica:started()
  self.status = 'streaming'
  self.stats.error = nil
  self:say('info', 'streaming from %s', self.stats.position ~= '' and self.stats.position or '(start)')
end

function Replica:heartbeat()
  self.stats.heartbeat_time = clock.time()
  self.stats.lag = 0
  self:flush()
end

-- Applies a finished source transaction with its position in one box
-- transaction; when a trigger of a target yields, statement by statement and
-- the position last (counted in split). Transactions without changes only
-- move the position, which is written at most once a second
function Replica:commit(changes, state, event_time)
  local stats = self.stats
  stats.position = state
  stats.event_time = event_time
  if #changes == 0 then
    self.dirty = true
    if clock.time() - (self.stored or 0) >= 1 then
      self:flush()
    end
    return
  end
  local statements = {}
  for _, change in ipairs(changes) do
    for _, target in ipairs(change[1].targets) do
      table.insert(statements, { target, change[2], change[3], change[4] })
    end
  end
  local atomic = store.apply(statements, function (item)
    item[1]:apply(item[2], item[3], item[4])
  end, function ()
    self.position:put(state, event_time)
  end)
  if not atomic then
    stats.split = stats.split + 1
  end
  self.dirty = false
  self.stored = clock.time()
  stats.applied_time = self.stored
  stats.lag = math.max(0, self.stored - event_time)
  stats.transactions = stats.transactions + 1
  stats.rows = stats.rows + #changes
end

-- Resolves the target spaces and opens the position space. It runs in the
-- fiber, so start() may be called before other code has made the spaces;
-- a missing space is retried with the connection.
function Replica:bind()
  for _, source in ipairs(self.order) do
    source.targets = {}
    for _, spec in ipairs(source.specs) do
      table.insert(source.targets, store.target(spec.target, spec.columns))
    end
  end
  self.position = store.position(self.config.position, self.config.key, self.config.name)
  self.bound    = true
end

function Replica:session(host, port)
  if not self.bound then
    self:bind()
  end
  local connection = self.driver.connect(self.config, host, port)
  self.connection = connection
  self.host, self.port = host, port
  self.stats.tls = connection.secure
  self:say('info', 'connected to %s:%d, %s%s', host, port, connection.version, connection.secure and ', TLS' or '')

  for _, source in ipairs(self.order) do
    self:describe(connection, source)
  end

  local state = self.position:get()
  if state == nil or self.resync_needed then
    state = self:resync(connection)
  end
  self.stats.position = state
  self.dirty = false

  connection:stream(state, self.sources, self)
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
    local host, port = address(hosts[index], self.driver.PORT)
    local status, problem = xpcall(self.session, trace, self, host, port)
    if self.connection ~= nil then
      self.connection:close()
      self.connection = nil
    end
    if box.is_in_txn() then
      box.rollback()
    end
    -- stop() or a cancel from outside, e.g. Tarantool shutting down
    if self.stopping or not pcall(fiber.testcancel) then
      break
    end
    self.stats.error = tostring(problem)
    if type(problem) == 'table' and problem.fatal then
      self.status = 'failed'
      self:say('error', 'stopped: %s', tostring(problem))
      break
    end
    if type(problem) == 'table' and problem.reload then
      self.resync_needed = true
      self:say('warn', '%s:%d: full reload: %s', host, port, tostring(problem))
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
    lag = metrics.gauge('mirrorflayer_lag_seconds', 'Delay of the last applied source transaction'),
    event = metrics.gauge('mirrorflayer_event_timestamp_seconds', 'Source time of the last applied source transaction'),
    applied = metrics.gauge('mirrorflayer_applied_timestamp_seconds', 'Local time of the last applied source transaction')
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
  local config = self.config
  check(type(config.hosts) == 'table' and #config.hosts > 0, 'config.hosts must be a non-empty array')
  check(type(config.user) == 'string', 'config.user must be a string')
  check(config.password == nil or type(config.password) == 'string', 'config.password must be a string')
  local problem = self.driver.check(config)
  check(problem == nil, '%s', problem)
  self.bound    = false
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

-- Reloads every table: restarts the replication with a full load, written
-- over the spaces as usual, so they never go empty
function Replica:reload()
  check(self.fiber ~= nil, 'not started')
  self.resync_needed = true
  self:stop()
  return self:start()
end

-- Returns the state: status, host, position (of the source), event_time (source
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
