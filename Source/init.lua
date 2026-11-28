-- MirrorFlayer: replication of source tables into Tarantool spaces, in a
-- fiber of Tarantool itself. The source is a driver: MirrorFlayer.MySQL
-- (MySQL and MariaDB over the binlog) or MirrorFlayer.PostgreSQL (logical
-- replication with pgoutput). MirrorFlayer.Runner keeps the loop with the
-- source: hosts, retries, sessions, loads and the stream, in a fiber of TX or
-- of an application thread (config.thread); the replicator here applies to
-- the spaces: targets, transactions, positions, statistics and metrics.
--
--   local MirrorFlayer = require('MirrorFlayer')
--   local replica = MirrorFlayer.new({
--     hosts = { 'db1:3306', 'db2:3306' }, user = 'replica', password = '...',
--     server = 1001
--   })
--   replica:map('Registry', 'Locations', 'GlobalLocations', { 'ID', 'Node', 'Kind', 'Link', 'Slot', 'Modified' })
--   replica:start()
--
-- handlers, the second argument of new(), are functions of the application
-- kept in TX, apart from the configuration, which stays plain data:
--   discover(replica, database, table, columns)
--                       an unmapped database (table nil) or table met in the
--                       stream; true: the application handles it (map() and
--                       reconnect(), asked again after a reconnect while it is
--                       not mapped), anything else ignores it until stop()
--   fail(replica, problem)
--                       a session failed: problem is { message, code, host,
--                       port, fatal, reload }; fatal: the replicator stopped,
--                       otherwise it connects again after retry

local fiber = require('fiber')
local clock = require('clock')
local log   = require('log')

local store  = require('MirrorFlayer.Store')
local Runner = require('MirrorFlayer.Runner')

local DRIVERS = Runner.DRIVERS

local DEFAULTS = {
  type      = 'mysql',
  name      = 'MirrorFlayer',
  position  = 'ReplicatorPositions',
  tls       = false,
  compress  = true,
  datetime  = 'datetime',
  json      = 'map',
  uuid      = 'uuid',
  heartbeat = 10,
  check     = 30,
  timeout   = 30,
  batch     = 100,
  retry     = 5,
  thread_id = 1
}

-- Handlers new() takes
local HANDLERS = { discover = true, fail = true }

local Replica   = {}
Replica.__index = Replica

-- Replicators running in this instance: by the name, by the position space
-- and the key; two of them must not share either
local running = { names = {}, keys = {} }

local function slot(config)
  return config.position .. '\0' .. type(config.key) .. ':' .. tostring(config.key)
end

local function check(condition, format, ...)
  if not condition then
    error(string.format(format, ...), 3)
  end
end

-- Creates a replicator; the connection settings are checked by start().
-- config: type ('mysql', the default, or 'postgresql'), hosts (array of
-- 'host[:port]'), user, password, server (replica server id of MySQL and
-- MariaDB), database, slot and publication (of PostgreSQL), tls (false, true
-- or { verify = 'none' | 'ca' | 'identity', ca, cert, key }), public_key (PEM
-- of the source for caching_sha2_password without TLS, asked from the server
-- when missing), compress (the compressed protocol of MySQL and MariaDB when
-- the server has it, true by default), position (space name), key (of the position, the server id
-- of MySQL or the slot of PostgreSQL by default), name (in logs and metrics),
-- datetime ('datetime' or 'number'), json ('map' or 'string'), uuid ('uuid',
-- 'binary' or 'string'), heartbeat (seconds), check (seconds between checks
-- that a node of Galera is ready), timeout (seconds), batch (rows per
-- transaction), retry (seconds between connection attempts), thread (a group
-- of application threads to run the source side in) and thread_id (which
-- thread of it, 1 by default). handlers: see the top of the file
local function new(config, handlers)
  check(type(config) == 'table', 'config must be a table')
  check(handlers == nil or type(handlers) == 'table', 'handlers must be a table')
  local functions = {}
  for key, value in pairs(handlers or {}) do
    check(HANDLERS[key], 'unknown handler %s', tostring(key))
    check(type(value) == 'function', 'handlers.%s must be a function', key)
    functions[key] = value
  end
  local options = {}
  for key, value in pairs(DEFAULTS) do
    options[key] = value
  end
  for key, value in pairs(config) do
    options[key] = value
  end
  check(DRIVERS[options.type] ~= nil, "config.type must be 'mysql' or 'postgresql'")
  local driver = require(DRIVERS[options.type])
  check(type(options.position) == 'string', 'config.position must be a space name')
  check(type(options.name) == 'string', 'config.name must be a string')
  check(options.datetime == 'datetime' or options.datetime == 'number', "config.datetime must be 'datetime' or 'number'")
  check(options.json == 'map' or options.json == 'string', "config.json must be 'map' or 'string'")
  check(type(options.check) == 'number' and options.check > 0, 'config.check must be a positive number of seconds')
  check(options.uuid == 'uuid' or options.uuid == 'binary' or options.uuid == 'string',
    "config.uuid must be 'uuid', 'binary' or 'string'")
  check(options.thread == nil or (type(options.thread) == 'string' and options.thread ~= ''),
    'config.thread must be the name of a group of application threads')
  check(type(options.thread_id) == 'number' and options.thread_id >= 1 and options.thread_id == math.floor(options.thread_id),
    'config.thread_id must be a positive integer')
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
  check(type(options.compress) == 'boolean', 'config.compress must be a boolean')
  if options.key == nil then
    options.key = driver.key(options)
  end
  check((type(options.key) == 'number' and options.key >= 0 and options.key == math.floor(options.key)) or
    type(options.key) == 'string', 'config.key must be an unsigned integer or a string')
  return setmetatable({
    config   = options,
    handlers = functions,
    driver   = driver,
    sources  = {},
    order    = {},
    loaders  = {},
    pending  = {},
    ignored  = { databases = {}, tables = {} },
    status   = 'created',
    stats   = { transactions = 0, rows = 0, loads = 0, fills = 0, split = 0 }
  }, Replica)
end

-- Maps a source table to a target: a space name or function (operation, old,
-- new) called inside the transaction ('insert' | 'update' | 'delete', and
-- 'reset' before a full reload). columns are source column names in tuple
-- order; by default the field names of the space format. On a running
-- replicator it takes effect at the next connection (reconnect()).
function Replica:map(database, table_name, target, columns)
  check(type(database) == 'string' and type(table_name) == 'string', 'database and table must be strings')
  check(type(target) == 'string' or type(target) == 'function', 'target must be a space name or function')
  check(columns == nil or type(columns) == 'table', 'columns must be an array of names')
  local id     = database .. '.' .. table_name
  local source = self.sources[id]
  if source == nil then
    source           = { id = id, database = database, table = table_name, specs = {} }
    self.sources[id] = source
    table.insert(self.order, source)
  end
  table.insert(source.specs, { target = target, columns = columns })
  self.bound = false
  return self
end

function Replica:say(level, format, ...)
  log[level]('MirrorFlayer[%s]: ' .. format, self.config.name, ...)
end

-- Sink of the Runner: the spaces ------------------------------------------------------

-- Resolves the target spaces and opens the position space. The Runner asks
-- for it with every session, so start() may be called before other code has
-- made the spaces; a missing space is retried with the connection.
-- A session starts: a reconnect() asked for is done, the targets are bound
-- again after map()
function Replica:bind()
  self.reconnecting = false
  if self.bound then
    return
  end
  for _, source in ipairs(self.order) do
    source.targets = {}
    for _, spec in ipairs(source.specs) do
      table.insert(source.targets, store.target(spec.target, spec.columns))
    end
  end
  self.positions = store.position(self.config.position, self.config.key, self.config.name, self.config.type)
  self.bound     = true
end

-- The mapped tables { id, database, table } in their order for a session, and
-- whether unmapped ones go to handlers.discover
function Replica:specs()
  local specs = {}
  for index, source in ipairs(self.order) do
    specs[index] = { id = source.id, database = source.database, table = source.table }
  end
  return specs, self.handlers.discover ~= nil
end

-- An unmapped database (table_name nil, none of its tables mapped) or table
-- met in the stream: handlers.discover decides once, true when the
-- application handles it, anything else ignores it until stop(). A table
-- mapped since, waiting for a reconnect, is not asked about
function Replica:unmapped(database, table_name, columns)
  local ignored, key = self.ignored.databases, database
  if table_name then
    ignored, key = self.ignored.tables, database .. '.' .. table_name
    if self.sources[key] then
      return true
    end
  else
    for _, source in ipairs(self.order) do
      if source.database == database then
        return true
      end
    end
  end
  if ignored[key] then
    return false
  end
  local status, result = pcall(self.handlers.discover, self, database, table_name, columns)
  if not status then
    self:say('error', 'handlers.discover for %s: %s', key, tostring(result))
    return false
  end
  if result ~= true then
    ignored[key] = true
  end
  return result == true
end

-- A session failed: handlers.fail gets { message, code, host, port, fatal,
-- reload }
function Replica:failed(problem)
  local handler = self.handlers.fail
  if not handler then
    return
  end
  local status, result = pcall(handler, self, problem)
  if not status then
    self:say('error', 'handlers.fail: %s', tostring(result))
  end
end

-- Binds the targets of a table to its columns
function Replica:columns(id, columns)
  local source = self.sources[id]
  for _, target in ipairs(source.targets) do
    target:bind(columns)
  end
  source.columns = columns
end

function Replica:position()
  return self.positions:get()
end

function Replica:flags()
  return self.reload_needed == true, self.flush_needed == true
end

function Replica:request_reload()
  self.reload_needed = true
end

-- Why a table is loaded alone before the stream, or nil: reconnect() asked
-- for it, or it has spaces among its targets and all of them are empty
function Replica:needs(id)
  if self.pending[id] then
    return 'asked for by reconnect()'
  end
  local spaces = 0
  for _, target in ipairs(self.sources[id].targets) do
    if target.space ~= nil then
      if target.space:len() > 0 then
        return nil
      end
      spaces = spaces + 1
    end
  end
  return spaces > 0 and 'its spaces are empty' or nil
end

-- A table is loaded over its targets; with flush its spaces are emptied first
-- (truncate, no on_replace triggers)
function Replica:load_begin(id, flush)
  local source = self.sources[id]
  if flush then
    for _, target in ipairs(source.targets) do
      if target.space ~= nil then
        target.space:truncate()
      end
    end
  end
  self.loaders[id] = { loader = store.load(source.targets, self.config.batch), aborts = store.aborts() }
end

function Replica:load_rows(id, rows)
  local loader = self.loaders[id].loader
  for _, row in ipairs(rows) do
    loader:add(row)
  end
end

-- Returns rows written, stale rows deleted, transactions aborted by triggers
function Replica:load_finish(id)
  local item       = self.loaders[id]
  self.loaders[id] = nil
  local deleted    = item.loader:finish()
  return item.loader.written, deleted, store.aborts() - item.aborts
end

function Replica:loaded(state)
  store.transaction(function ()
    self.positions:put(state, clock.time())
  end)
  self.stats.loads    = self.stats.loads + 1
  self.stats.position = state
  self.reload_needed  = false
  self.flush_needed   = false
  self.pending        = {}
end

function Replica:filled(id)
  self.pending[id] = nil
  self.stats.fills = self.stats.fills + 1
end

function Replica:resume(state)
  self.stats.position = state
  self.dirty          = false
end

-- status, error (an empty string clears it), host, port, tls
function Replica:report(fields)
  if fields.status ~= nil then
    self.status = fields.status
  end
  if fields.error ~= nil then
    self.stats.error = fields.error ~= '' and fields.error or nil
  end
  if fields.host ~= nil then
    self.host, self.port = fields.host, fields.port
  end
  if fields.tls ~= nil then
    self.stats.tls, self.stats.compressed = fields.tls, fields.compressed
  end
end

function Replica:cleanup()
  if box.is_in_txn() then
    box.rollback()
  end
end

-- Stream -------------------------------------------------------------------------

function Replica:flush()
  if not self.dirty then
    return
  end
  store.transaction(function ()
    self.positions:put(self.stats.position, self.stats.event_time or 0)
  end)
  self.dirty  = false
  self.stored = clock.time()
end

-- The driver has asked the source for the stream
function Replica:started()
  self.status      = 'streaming'
  self.stats.error = nil
end

-- The source moved on without changes for the mapped tables (a keepalive of
-- PostgreSQL); the position is written at most once a second
-- advance(), heartbeat() and commit() return true when reconnect() asks to
-- end the session
function Replica:advance(state)
  self.stats.position = state
  self.dirty          = true
  if clock.time() - (self.stored or 0) >= 1 then
    self:flush()
  end
  return self.reconnecting
end

-- Prepared XA transactions wait for their decision in the space named after
-- the position space with XA, { key, xid, events }
function Replica:waiting(create)
  if self.xa == nil then
    self.xa = store.prepared(self.config.position .. 'XA', self.config.key, create)
  end
  return self.xa
end

-- XA PREPARE: the events of the transaction are kept with the position in one
-- box transaction, they are applied by its XA COMMIT
function Replica:prepare(xid, events, state, event_time)
  local stats      = self.stats
  local waiting    = self:waiting(true)
  stats.position   = state
  stats.event_time = event_time
  store.transaction(function ()
    waiting:put(xid, events)
    self.positions:put(state, event_time)
  end)
  self.dirty         = false
  self.stored        = clock.time()
  stats.applied_time = self.stored
end

function Replica:prepared(xid)
  local waiting = self:waiting(false)
  return waiting and waiting:get(xid)
end

function Replica:heartbeat()
  self.stats.heartbeat_time = clock.time()
  self.stats.lag            = 0
  self:flush()
  return self.reconnecting
end

-- Applies a finished source transaction with its position in one box
-- transaction; when a trigger of a target yields, statement by statement and
-- the position last (counted in split). Transactions without changes only
-- move the position, which is written at most once a second. A change is
-- { table id, operation, old, new }
function Replica:commit(changes, state, event_time, xid)
  local stats      = self.stats
  stats.position   = state
  stats.event_time = event_time
  if #changes == 0 and xid == nil then
    self.dirty = true
    if clock.time() - (self.stored or 0) >= 1 then
      self:flush()
    end
    return self.reconnecting
  end
  local statements = {}
  for _, change in ipairs(changes) do
    for _, target in ipairs(self.sources[change[1]].targets) do
      table.insert(statements, { target, change[2], change[3], change[4] })
    end
  end
  local waiting = xid ~= nil and self:waiting(false) or nil
  local atomic  = store.apply(statements, function (item)
    item[1]:apply(item[2], item[3], item[4])
  end, function ()
    self.positions:put(state, event_time)
    if waiting ~= nil then
      waiting:delete(xid)
    end
  end)
  if not atomic then
    stats.split = stats.split + 1
  end
  self.dirty         = false
  self.stored        = clock.time()
  stats.applied_time = self.stored
  stats.lag          = math.max(0, self.stored - event_time)
  stats.transactions = stats.transactions + 1
  stats.rows         = stats.rows + #changes
  return self.reconnecting
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
    lag     = metrics.gauge('mirrorflayer_lag_seconds', 'Delay of the last applied source transaction'),
    event   = metrics.gauge('mirrorflayer_event_timestamp_seconds', 'Source time of the last applied source transaction'),
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
  check(not self.running, 'already started')
  check(type(box.cfg) ~= 'function', 'call box.cfg{} before start()')
  check(#self.order > 0 or self.handlers.discover, 'nothing is mapped and there is no handlers.discover')
  local config = self.config
  check(type(config.hosts) == 'table' and #config.hosts > 0, 'config.hosts must be a non-empty array')
  check(type(config.user) == 'string', 'config.user must be a string')
  check(config.password == nil or type(config.password) == 'string', 'config.password must be a string')
  local problem = self.driver.check(config)
  check(problem == nil, '%s', problem)
  local other = running.names[config.name]
  check(other == nil or other == self, 'a replicator named %s is running, give config.name of its own', config.name)
  other = running.keys[slot(config)]
  check(other == nil or other == self,
    'a replicator keeps its position in %s under %s already, give config.key of its own', config.position,
    tostring(config.key))
  local thread
  if config.thread ~= nil then
    thread  = require('MirrorFlayer.Thread')
    problem = thread.check(config.thread, config.thread_id)
    check(problem == nil, '%s', problem)
  end
  self.bound        = false
  self.loaders      = {}
  self.pending      = {}
  self.ignored      = { databases = {}, tables = {} }
  self.reconnecting = false
  self.status       = 'starting'
  store.configure(config.batch)
  self:register()
  if thread ~= nil then
    thread.launch(self)
    self.thread = thread
  else
    self.runner = Runner.new(config, self)
    self.fiber  = self.runner:start()
  end
  self.running               = true
  running.names[config.name] = self
  running.keys[slot(config)] = self
  return self
end

function Replica:stop()
  if not self.running then
    return
  end
  if self.thread ~= nil then
    self.thread.halt(self)
    self.thread = nil
  else
    self.runner:stop()
    self.fiber = nil
  end
  self.running                    = false
  running.names[self.config.name] = nil
  running.keys[slot(self.config)] = nil
  self:cleanup()
  self:unregister()
  self.status = 'stopped'
end

-- Reloads every table: restarts the replication with a full load, written
-- over the spaces, so they never go empty; rows gone from the source are
-- deleted afterwards. With flush the spaces of each table are emptied
-- (truncate, no on_replace triggers) right before it is loaded
function Replica:reload(flush)
  check(self.running, 'not started')
  self.reload_needed = true
  self.flush_needed  = flush == true
  self:stop()
  return self:start()
end

-- Ends the session after the current source transaction (at the next
-- heartbeat when the source is idle) and connects again at once, without a
-- pause: the tables are described again, the ones map() added since come in,
-- and the tables listed by their 'database.table' ids are loaded alone before
-- the stream, which comes from the stored position over them
function Replica:reconnect(tables)
  check(self.running, 'not started')
  check(tables == nil or type(tables) == 'table', 'tables must be an array of database.table ids')
  for _, id in ipairs(tables or {}) do
    check(self.sources[id], '%s is not mapped', tostring(id))
  end
  for _, id in ipairs(tables or {}) do
    self.pending[id] = true
  end
  self.reconnecting = true
  return self
end

-- Returns the state: status, host, position (of the source), event_time (source
-- time of the last applied transaction), applied_time, lag (seconds between
-- the two, 0 after a heartbeat), the counters transactions, rows, loads (full
-- loads), fills (tables loaded alone into empty spaces), split and aborts, and
-- the last error
function Replica:stat()
  local stats = self.stats
  return {
    name           = self.config.name,
    status         = self.status,
    host           = self.host and string.format('%s:%d', self.host, self.port) or nil,
    position       = stats.position,
    event_time     = stats.event_time,
    applied_time   = stats.applied_time,
    heartbeat_time = stats.heartbeat_time,
    lag            = stats.lag,
    transactions   = stats.transactions,
    rows           = stats.rows,
    loads          = stats.loads,
    fills          = stats.fills,
    tls            = stats.tls,
    compressed     = stats.compressed,
    split          = stats.split,
    aborts         = store.aborts(),
    error          = stats.error
  }
end

return {
  new      = new,
  _VERSION = '3.1.0'
}
