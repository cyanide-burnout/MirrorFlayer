-- MirrorFlayer.Runner: the replication loop of one replicator, the side that
-- talks to the source: hosts, retries, the session of a driver, the columns
-- of the tables, full loads and the stream. It runs in a fiber of TX, or of
-- an application thread (config.thread), and touches no space: whatever
-- concerns the spaces goes to a sink, the replicator itself in TX or a proxy
-- to it from the thread (MirrorFlayer.Thread).
--
-- The sink:
--   bind()                          resolve the targets and the position space
--   columns(id, columns)            bind the targets of a table to its columns
--   position()                      the stored position or nil
--   flags()                         reload, flush: a full load is asked for
--   load_begin(id, flush)           a table is loaded, its spaces emptied first
--   load_rows(id, rows)             rows of it
--   load_finish(id)                 written, deleted, aborts
--   loaded(state)                   the full load is done at the position
--   empty(id)                       its spaces are all empty
--   filled()                        a table was loaded alone
--   resume(state)                   the stream starts at the position
--   started()                       the source streams
--   commit(changes, state, time, xid)
--                                   a transaction: { id, operation, old, new }; xid
--                                   the prepared XA transaction it decides
--   prepare(xid, events, state, time)
--                                   an XA transaction prepared, its events wait
--   prepared(xid)                   the events of a prepared XA transaction or nil
--   advance(state)                  the position moves without changes
--   heartbeat()                     the source is there
--   report(fields)                  status, error, host, port, tls, compressed
--   request_reload()                the position is lost, a full load comes
--   cleanup()                       after a failed session

local fiber = require('fiber')
local clock = require('clock')
local log   = require('log')

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

-- specs: { id, database, table } of the mapped tables in their order
local function new(config, specs, sink)
  local self = setmetatable({
    config  = config,
    driver  = require(DRIVERS[config.type]),
    sink    = sink,
    sources = {},
    order   = {}
  }, Runner)
  for _, spec in ipairs(specs) do
    local source            = { id = spec.id, database = spec.database, table = spec.table }
    self.sources[source.id] = source
    table.insert(self.order, source)
  end
  return self
end

function Runner:say(level, format, ...)
  log[level]('MirrorFlayer[%s]: ' .. format, self.config.name, ...)
end

-- Tables -------------------------------------------------------------------------

-- Reads the columns of the source tables (in one query of the driver) and
-- binds the targets to them
function Runner:describe(connection, sources)
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

-- Loads alone the tables whose spaces are all empty though the position is
-- there (a space made anew or emptied by hand). The stream then comes from
-- the older position over the newer rows and converges, applying is
-- idempotent; the table may be empty at the source too. All of them in one query
function Runner:refill(connection)
  local empty = {}
  for _, source in ipairs(self.order) do
    if self.sink:empty(source.id) then
      self:say('info', '%s: its spaces are empty, loading the table', source.id)
      table.insert(empty, source)
    end
  end
  if #empty > 0 then
    self.sink:report({ status = 'loading' })
    self:fill(connection, empty, false)
    for _ = 1, #empty do
      self.sink:filled()
    end
  end
end

-- Handler of the stream of a driver --------------------------------------------------

function Runner:started()
  self.sink:started()
  self:say('info', 'streaming from %s', self.state ~= '' and self.state or '(start)')
end

function Runner:heartbeat()
  self.sink:heartbeat()
end

function Runner:advance(state)
  self.sink:advance(state)
end

-- Changes go to the sink by the id of their table
function Runner:commit(changes, state, event_time, xid)
  local items = {}
  for index, change in ipairs(changes) do
    items[index] = { change[1].id, change[2], change[3], change[4] }
  end
  self.sink:commit(items, state, event_time, xid)
end

function Runner:prepare(xid, events, state, event_time)
  self.sink:prepare(xid, events, state, event_time)
end

function Runner:prepared(xid)
  return self.sink:prepared(xid)
end

-- Session and loop -----------------------------------------------------------------

function Runner:session(host, port)
  self.sink:bind()
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
    pcall(sink.report, sink, { error = tostring(problem) })
    if type(problem) == 'table' and problem.fatal then
      failed = true
      pcall(sink.report, sink, { status = 'failed' })
      self:say('error', 'stopped: %s', tostring(problem))
      break
    end
    if type(problem) == 'table' and problem.reload then
      pcall(sink.request_reload, sink)
      self:say('warn', '%s:%d: full reload: %s', host, port, tostring(problem))
    else
      self:say('error', '%s:%d: %s', host, port, tostring(problem))
    end
    index = index % #hosts + 1
    pcall(sink.report, sink, { status = 'waiting' })
    if not pcall(fiber.sleep, self.config.retry) then
      break
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
