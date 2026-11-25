-- MirrorFlayer.Thread: replicators in an application thread (config.thread,
-- Tarantool 3.9 or later with app_threads). The thread runs the Runner: the
-- socket, TLS, the protocol of the source, the binlog or pgoutput and the
-- conversion of values. The spaces stay in TX: the sink of the Runner is a
-- proxy that calls the replicator in TX by its name through one exported
-- function, so TX only applies the rows and transactions.
--
-- threads.eval() runs code in every thread of a group: each thread starts
-- the Runner only when it is the one asked for (thread_id). Values cross as
-- MsgPack; a value an update did not send (Column.UNCHANGED) crosses as the
-- list of its positions in the row.

local threads = require('threads')

local Runner  = require('MirrorFlayer.Runner')
local errors  = require('MirrorFlayer.Failure')
local convert = require('MirrorFlayer.Column')

local NULL      = box.NULL
local UNCHANGED = convert.UNCHANGED

-- The function TX exports for the threads: (name, method, ...)
local EXPORT = 'mirrorflayer.call'

-- What TX evaluates in the threads: (path, cpath, command, name, argument)
local BOOT = [[
local path, cpath, command, name, argument = ...
local thread = package.loaded['MirrorFlayer.Thread']
if thread == nil then
  package.path, package.cpath = path, cpath
  thread = require('MirrorFlayer.Thread')
end
return thread[command](name, argument)
]]

-- Thread side ------------------------------------------------------------------------

local runners = {}

local function raise(problem)
  if type(problem) == 'table' then
    local failure  = errors.failure(problem.message or 'unknown error', problem.code, problem.fatal)
    failure.reload = problem.reload
    error(failure, 0)
  end
  error(tostring(problem), 0)
end

local function call(name, method, ...)
  -- The results come as { { results } }; dispatch() returns one table
  local result = threads.call('tx', EXPORT, { name, method, ... })[1][1]
  if result[1] then
    return unpack(result, 3, 2 + result[2])
  end
  raise(result[2])
end

-- Positions of the values an update did not send; they cross as NULL
local function marks(row)
  if row == nil then
    return NULL
  end
  local list
  for index = 1, #row do
    if rawequal(row[index], UNCHANGED) then
      list = list or {}
      table.insert(list, index)
      row[index] = NULL
    end
  end
  return list or NULL
end

local function remote(name)
  local sink = {
    commit = function (_, changes, state, event_time)
      for _, change in ipairs(changes) do
        local old, new = change[3], change[4]
        -- old and new may be one table (an update that kept the key)
        change[6] = marks(new)
        change[5] = rawequal(old, new) and change[6] or marks(old)
        change[3] = old or NULL
        change[4] = new or NULL
      end
      return call(name, 'commit', changes, state, event_time)
    end
  }
  return setmetatable(sink, {
    __index = function (_, method)
      return function (_, ...)
        return call(name, method, ...)
      end
    end
  })
end

-- Starts the Runner of a replicator when this is its thread: argument is
-- { config, sources, thread_id }. Returns true when it started here
local function start(name, argument)
  if threads.info().thread_id ~= argument.thread_id then
    return false
  end
  if runners[name] ~= nil then
    runners[name]:stop()
  end
  local runner = Runner.new(argument.config, argument.sources, remote(name))
  runner:start()
  runners[name] = runner
  return true
end

local function stop(name)
  local runner = runners[name]
  if runner == nil then
    return false
  end
  runners[name] = nil
  runner:stop()
  return true
end

-- TX side ----------------------------------------------------------------------------

local replicas = {}
local exported = false

local function pack(...)
  return { n = select('#', ...), ... }
end

local function describe(problem)
  if type(problem) == 'table' then
    return { message = problem.message or tostring(problem), code = problem.code, fatal = problem.fatal,
      reload = problem.reload }
  end
  return { message = tostring(problem) }
end

-- Rows of a change as the replicator in TX takes them
local function restore(change)
  for slot, marked in pairs({ [3] = 5, [4] = 6 }) do
    local row = change[slot]
    if row == nil then
      change[slot] = nil
    elseif change[marked] ~= nil then
      for _, index in ipairs(change[marked]) do
        row[index] = UNCHANGED
      end
    end
  end
end

-- The exported function: a method of the replicator of that name in TX;
-- returns { true, count, results... } or { false, failure }
local function dispatch(name, method, ...)
  local replica = replicas[name]
  if replica == nil then
    return { false, { message = string.format('replicator %s is not running', tostring(name)) } }
  end
  if method == 'commit' then
    for _, change in ipairs((...)) do
      restore(change)
    end
  end
  local result = pack(pcall(replica[method], replica, ...))
  if not result[1] then
    if box.is_in_txn() then
      box.rollback()
    end
    return { false, describe(result[2]) }
  end
  local answer = { true, result.n - 1 }
  for index = 2, result.n do
    answer[index + 1] = result[index]
  end
  return answer
end

local function export()
  if exported then
    return
  end
  local status, problem = pcall(threads.export, EXPORT, dispatch)
  if not status and not (type(problem) == 'cdata' and problem.name == 'FUNCTION_EXISTS') then
    error(problem, 0)
  end
  exported = true
end

local function eval(group, command, name, argument)
  return threads.eval(group, BOOT, { package.path, package.cpath, command, name, argument or {} })
end

-- Starts the Runner of a replica in its thread
local function launch(replica, specs)
  local config = replica.config
  export()
  replicas[config.name] = replica
  local results         = eval(config.thread, 'start', config.name,
    { config = config, sources = specs, thread_id = config.thread_id })
  for _, result in ipairs(results) do
    if result[1] == true then
      return
    end
  end
  replicas[config.name] = nil
  errors.fatal('thread %d of the group %s did not start the replicator', config.thread_id, config.thread)
end

local function halt(replica)
  local config = replica.config
  eval(config.thread, 'stop', config.name)
end

-- The group exists and has the thread
local function check(group, thread_id)
  local status, module = pcall(require, 'threads')
  if not status then
    return 'config.thread needs application threads of Tarantool 3.9 or later'
  end
  for _, item in ipairs(module.info().groups) do
    if item.name == group then
      if thread_id > item.size then
        return string.format('the group %s has %d threads, config.thread_id is %d', group, item.size, thread_id)
      end
      return nil
    end
  end
  return string.format('there is no thread group %s (box.cfg app_threads or threads.groups)', group)
end

return {
  start    = start,
  stop     = stop,
  launch   = launch,
  halt     = halt,
  check    = check,
  replicas = replicas
}
