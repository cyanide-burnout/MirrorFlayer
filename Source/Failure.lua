-- Errors raised by MirrorFlayer and its drivers: failure objects with a
-- message and, when known, the error code of the source. fatal stops the
-- replicator instead of reconnecting; reload makes the next connection start
-- with a full load (the position is lost, the table changed under the stream)

local Failure = {}
Failure.__index = Failure
Failure.__tostring = function (self)
  if self.code ~= nil then
    return string.format('%s (%s)', self.message, tostring(self.code))
  end
  return self.message
end

-- A raised error object; code is the error number or code of the source
local function failure(message, code, fatal)
  return setmetatable({ message = message, code = code, fatal = fatal }, Failure)
end

local function fatal(format, ...)
  error(failure(string.format(format, ...), nil, true), 0)
end

-- Raises a failure that asks for a full load
local function reload(format, ...)
  local problem = failure(string.format(format, ...))
  problem.reload = true
  error(problem, 0)
end

return {
  failure = failure,
  fatal = fatal,
  reload = reload
}
