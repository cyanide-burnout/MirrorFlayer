-- MirrorFlayer.Runner: the replication loop of one replicator, the side that
-- talks to the source: hosts, retries, the session of a driver, the columns
-- of the tables, full loads and the stream. It runs in a fiber of TX, or of
-- an application thread (config.thread), and touches no space: whatever
-- concerns the spaces goes to a sink, the replicator itself in TX or a proxy
-- to it from the thread (MirrorFlayer.Thread).
--
-- The sink:
--   bind()                          resolve the targets and the position space
--   specs()                         the mapped tables { id, database, table } of a
--                                   session; true when unmapped ones are asked about
--   unmapped(database, table, columns)
--                                   an unmapped database (table nil) or table met;
--                                   true when the application handles it
--   columns(id, columns)            bind the targets of a table to its columns
--   position()                      the stored position or nil
--   flags()                         reload, flush: a full load is asked for
--   load_begin(id, flush)           a table is loaded, its spaces emptied first
--   load_rows(id, rows)             rows of it
--   load_finish(id)                 written, deleted, aborts
--   loaded(state)                   the full load is done at the position
--   needs(id)                       why it is loaded alone before the stream, or nil
--   filled(id)                      it was loaded alone
--   resume(state)                   the stream starts at the position
--   started()                       the source streams
--   commit(changes, state, time, xid)
--                                   a transaction: { id, operation, old, new }; xid
--                                   the prepared XA transaction it decides; commit,
--                                   advance and heartbeat return true to reconnect
--   prepare(xid, events, state, time)
--                                   an XA transaction prepared, its events wait
--   prepared(xid)                   the events of a prepared XA transaction or nil
--   advance(state)                  the position moves without changes
--   heartbeat()                     the source is there
--   report(fields)                  status, error, host, port, tls, compressed
--   request_reload()                the position is lost, a full load comes
--   failed(problem)                 a session failed: { message, code, host, port,
--                                   fatal, reload }
--   cleanup()                       after a failed session

local fiber = require('fiber')
local clock = require('clock')
local log   = require('log')

local errors = require('MirrorFlayer.Failure')

-- Source drivers by config.type
local DRIVERS = {
  mysql      = 'MirrorFlayer.MySQL',
  postgresql = 'MirrorFlayer.PostgreSQL'
}

local Runner   = {}
Runner.__index = Runner

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

local function new(config, sink)
  return setmetatable({
    config  = config,
    driver  = require(DRIVERS[config.type]),
    sink    = sink,
    sources = {},
    order   = {}
  }, Runner)
end

function Runner:say(level, format, ...)
  log[level]('MirrorFlayer[%s]: ' .. format, self.config.name, ...)
end

-- Tables -------------------------------------------------------------------------

-- The mapped tables of a session from the sink: map() may have added some
function Runner:adopt()
  local specs, discovering = self.sink:specs()
  self.sources, self.order, self.databases = {}, {}, {}
  for _, spec in ipairs(specs) do
    local source            = { id = spec.id, database = spec.database, table = spec.table }
    self.sources[source.id] = source
    self.databases[source.database] = true
    table.insert(self.order, source)
  end
  self.discovering = discovering == true
  self.asked       = { databases = {}, tables = {} }
end

-- Reads the columns of the source tables (in one query of the driver) and
-- binds the targets to them
function Runner:describe(connection, sources)
  if #sources == 0 then
    return
  end
  local described = connection:describe(sources)
  for _, source in ipairs(sources) do
    self.sink:columns(source.id, described[source])
    source.columns = described[source]
    source.stale   = false
  end
end

-- Describes a table again over a connection of its own while the stream
-- holds the current one (the driver asks for it after DDL)
function Runner:refresh(source)
  local connection = self.driver.connect(self.config, self.host, self.port)
  local status, problem = pcall(self.describe, self, connection, { source })
  connection:close()
  if not status then
    error(problem, 0)
  end
end

-- Loads the tables over their targets in one query of the driver, one after
-- another; with flush their spaces are emptied first
function Runner:fill(connection, sources, flush)
  if #sources == 0 then
    return
  end
  local chunk = self.config.batch * 10
  local rows    = {}
  local count   = 0
  local current = 1
  self.sink:load_begin(sources[1].id, flush == true)
  connection:load(sources, function (source, row)
    table.insert(rows, row)
    count = count + 1
    if #rows >= chunk then
      self.sink:load_rows(source.id, rows)
      rows = {}
    end
  end, function (source)
    if #rows > 0 then
      self.sink:load_rows(source.id, rows)
      rows = {}
    end
    local written, deleted, aborts = self.sink:load_finish(source.id)
    self:say('info', '%s: %d rows loaded, %d written, %d stale rows deleted, %d aborted by triggers', source.id,
      count, written, deleted, aborts)
    count   = 0
    current = current + 1
    if sources[current] then
      self.sink:load_begin(sources[current].id, flush == true)
    end
  end)
end

-- Takes the position of the source first, then loads every table; changes
-- made meanwhile come again from the stream and applying them is idempotent
function Runner:load(connection, flush)
  local state   = connection:position()
  local started = clock.time()
  self.sink:report({ status = 'loading' })
  self:say('info', 'full load at %s%s', state ~= '' and state or '(start)', flush and ', spaces emptied' or '')
  self:fill(connection, self.order, flush)
  self.sink:loaded(state)
  self:say('info', 'full load done in %.1f s', clock.time() - started)
  return state
end

-- Loads alone the tables reconnect() asked for and the ones whose spaces are
-- all empty though the position is there (a space made anew or emptied by
-- hand). The stream then comes from the older position over the newer rows
-- and converges, applying is idempotent; the table may be empty at the source
-- too. All of them in one query
function Runner:refill(connection)
  local wanted = {}
  for _, source in ipairs(self.order) do
    local reason = self.sink:needs(source.id)
    if reason then
      self:say('info', '%s: %s, loading the table', source.id, reason)
      table.insert(wanted, source)
    end
  end
  if #wanted > 0 then
    self.sink:report({ status = 'loading' })
    self:fill(connection, wanted, false)
    for _, source in ipairs(wanted) do
      self.sink:filled(source.id)
    end
  end
end

-- Columns of an unmapped table as { name, type } over a connection of its
-- own, as refresh() does; nil when it cannot be described (dropped meanwhile)
function Runner:inspect(source)
  local status, described = pcall(function ()
    local connection = self.driver.connect(self.config, self.host, self.port)
    local done, result = pcall(connection.describe, connection, { source })
    connection:close()
    if not done then
      error(result, 0)
    end
    return result[source]
  end)
  if not status then
    self:say('warn', '%s: not described: %s', source.id, tostring(described))
    return nil
  end
  local columns = {}
  for index, item in ipairs(described) do
    columns[index] = { name = item.name, type = item.type }
  end
  return columns
end

-- Handler of the stream of a driver --------------------------------------------------

function Runner:started()
  self.sink:started()
  self:say('info', 'streaming from %s', self.state ~= '' and self.state or '(start)')
end

-- The sink answers heartbeat, advance and commit with true when reconnect()
-- asks to end the session: the position is stored by then
function Runner:heartbeat()
  if self.sink:heartbeat() then
    errors.reconnect()
  end
end

function Runner:advance(state)
  if self.sink:advance(state) then
    errors.reconnect()
  end
end

-- Changes go to the sink by the id of their table
function Runner:commit(changes, state, event_time, xid)
  local items = {}
  for index, change in ipairs(changes) do
    items[index] = { change[1].id, change[2], change[3], change[4] }
  end
  if self.sink:commit(items, state, event_time, xid) then
    errors.reconnect()
  end
end

function Runner:prepare(xid, events, state, event_time)
  self.sink:prepare(xid, events, state, event_time)
end

function Runner:prepared(xid)
  return self.sink:prepared(xid)
end

-- An unmapped table met in the stream: once a session the sink decides on its
-- database when none of its tables is mapped, then on the table with its
-- columns. Nothing is asked without handlers.discover
function Runner:unmapped(database, table_name)
  if not self.discovering then
    return
  end
  local asked = self.asked
  if asked.databases[database] == nil then
    asked.databases[database] = self.databases[database] == true or self.sink:unmapped(database) == true
  end
  local id = database .. '.' .. table_name
  if not asked.databases[database] or asked.tables[id] then
    return
  end
  asked.tables[id] = true
  self.sink:unmapped(database, table_name, self:inspect({ id = id, database = database, table = table_name }))
end

-- Session and loop -----------------------------------------------------------------

function Runner:session(host, port)
  self.sink:bind()
  self:adopt()
  local connection = self.driver.connect(self.config, host, port)
  self.connection  = connection
  self.host, self.port = host, port
  self.sink:report({ host = host, port = port, tls = connection.secure == true, compressed = connection.compressed == true })
  self:say('info', 'connected to %s:%d, %s%s%s', host, port, connection.version, connection.secure and ', TLS' or '',
    connection.compressed and ', compressed with ' .. tostring(connection.algorithm) or '')

  self:describe(connection, self.order)

  local state          = self.sink:position()
  local reload, flush  = self.sink:flags()
  if state == nil or reload then
    state = self:load(connection, flush)
  else
    self:refill(connection)
  end
  self.state = state
  self.sink:resume(state)

  connection:stream(state, self.sources, self)
end

-- A failure as plain data for the sink, also across threads
local function described(problem, host, port)
  local item = { message = tostring(problem), host = host, port = port, fatal = false, reload = false }
  if type(problem) == 'table' then
    item.message = problem.message or item.message
    item.code    = problem.code
    item.fatal   = problem.fatal == true
    item.reload  = problem.reload == true
  end
  return item
end

-- Keeps failure objects, adds the stack to unexpected errors
local function trace(problem)
  if type(problem) == 'table' then
    return problem
  end
  return debug.traceback(tostring(problem), 2)
end

function Runner:loop()
  local hosts  = self.config.hosts
  local index  = 1
  local failed = false
  while not self.stopping do
    local host, port = address(hosts[index], self.driver.PORT)
    local status, problem = xpcall(self.session, trace, self, host, port)
    if self.connection ~= nil then
      pcall(self.connection.close, self.connection)
      self.connection = nil
    end
    -- stop() or a cancel from outside, e.g. Tarantool shutting down
    if self.stopping or not pcall(fiber.testcancel) then
      break
    end
    -- The sink of a thread may fail too, the loop goes on
    local sink = self.sink
    pcall(sink.cleanup, sink)
    if type(problem) == 'table' and problem.reconnect then
      -- reconnect(): the same host at once, no error
      self:say('info', '%s:%d: reconnecting', host, port)
    else
      pcall(sink.report, sink, { error = tostring(problem) })
      local fatal = type(problem) == 'table' and problem.fatal
      if fatal then
        failed = true
        pcall(sink.report, sink, { status = 'failed' })
        self:say('error', 'stopped: %s', tostring(problem))
      elseif type(problem) == 'table' and problem.reload then
        pcall(sink.request_reload, sink)
        self:say('warn', '%s:%d: full reload: %s', host, port, tostring(problem))
      else
        self:say('error', '%s:%d: %s', host, port, tostring(problem))
      end
      pcall(sink.failed, sink, described(problem, host, port))
      if fatal then
        break
      end
      index = index % #hosts + 1
      pcall(sink.report, sink, { status = 'waiting' })
      if not pcall(fiber.sleep, self.config.retry) then
        break
      end
    end
  end
  if not failed then
    pcall(self.sink.report, self.sink, { status = 'stopped' })
  end
end

function Runner:start()
  self.stopping = false
  self.fiber    = fiber.new(self.loop, self)
  self.fiber:name('MirrorFlayer:' .. self.config.name, { truncate = true })
  self.fiber:set_joinable(true)
  return self.fiber
end

function Runner:stop()
  if self.fiber == nil then
    return
  end
  self.stopping = true
  if self.fiber:status() ~= 'dead' then
    self.fiber:cancel()
  end
  pcall(self.fiber.join, self.fiber)
  self.fiber = nil
end

return {
  DRIVERS = DRIVERS,
  new     = new
}
