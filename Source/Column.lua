-- Values in their Tarantool types, the same for every source driver.
--
-- Integers are numbers (uint64_t/int64_t cdata above 2^53), zero dates and
-- NULL are box.NULL, binary strings are varbinary (MP_BIN).
-- Date and time follow the datetime mode of the replicator, all in UTC:
--   'datetime'  datetime of Tarantool with fractions, time of day and
--               durations as interval (the default)
--   'number'    seconds since the Unix epoch with fractions, durations in
--               seconds
-- JSON follows the json mode:
--   'map'       MsgPack maps and arrays, JSON null as box.NULL (the default)
--   'string'    the text as the server prints it
-- UUID follows the uuid mode:
--   'uuid'      uuid of Tarantool (the default)
--   'binary'    16 bytes in network byte order as varbinary
--   'string'    the canonical text, lowercase

local ffi      = require('ffi')
local uuid     = require('uuid')
local datetime = require('datetime')
local json     = require('json')

local errors = require('MirrorFlayer.Failure')

local sub = string.sub

local NULL = box.NULL

local status, varbinary = pcall(require, 'varbinary')
local binary
if status and varbinary.new ~= nil then
  binary = varbinary.new
else
  binary = function (value) return value end
end

local function integer(text)
  local value = tonumber(text)
  if value ~= nil and value < 2^53 and value > -2^53 then
    return value
  end
  value = tonumber64(text)
  if value == nil then
    error(errors.failure('bad integer ' .. text), 0)
  end
  return value
end

local function float(value)
  return tonumber(ffi.new('float', value))
end

-- A moment in UTC, as the datetime mode of the column wants it; zero dates
-- become NULL
local function moment(column, year, month, day, hour, minute, second, usec)
  if year == 0 and month == 0 and day == 0 then
    return NULL
  end
  local value = datetime.new({ year = year, month = month, day = day, hour = hour, min = minute,
    sec = second, nsec = usec * 1000, tzoffset = 0 })
  if column.datetime == 'number' then
    return value.epoch + usec / 1e6
  end
  return value
end

-- The same for seconds since the Unix epoch
local function instant(column, seconds, usec)
  if seconds == 0 and usec == 0 then
    return NULL
  end
  if column.datetime == 'number' then
    return seconds + usec / 1e6
  end
  return datetime.new({ timestamp = seconds, nsec = usec * 1000, tzoffset = 0 })
end

-- A duration: interval of Tarantool, or seconds in the number mode; negative
-- is the sign of the whole duration
local function duration(column, negative, hours, minutes, seconds, usec)
  local sign = negative and -1 or 1
  if column.datetime == 'number' then
    return sign * (hours * 3600 + minutes * 60 + seconds + usec / 1e6)
  end
  return datetime.interval.new({ hour = sign * hours, min = sign * minutes, sec = sign * seconds,
    nsec = sign * usec * 1000 })
end

local function microseconds(fraction)
  return fraction ~= '' and tonumber(sub(fraction .. '000000', 1, 6)) or 0
end

-- Parses '[-]H+:MM:SS[.ffffff]'
local function clock(column, text)
  local sign, hours, minutes, seconds, fraction = string.match(text, '^(%-?)(%d+):(%d+):(%d+)%.?(%d*)')
  if hours == nil then
    error(errors.failure('bad time ' .. text), 0)
  end
  return duration(column, sign == '-', tonumber(hours), tonumber(minutes), tonumber(seconds), microseconds(fraction))
end

-- Parses 'YYYY-MM-DD[ HH:MM:SS[.ffffff]]', a time zone after it is not read:
-- the session of the driver is in UTC
local function temporal(column, text)
  local year, month, day, hour, minute, second, fraction =
    string.match(text, '^(%d+)-(%d+)-(%d+)[ T]?(%d*):?(%d*):?(%d*)%.?(%d*)')
  if year == nil then
    error(errors.failure('bad date ' .. text), 0)
  end
  return moment(column, tonumber(year), tonumber(month), tonumber(day),
    tonumber(hour) or 0, tonumber(minute) or 0, tonumber(second) or 0, microseconds(fraction))
end

-- JSON text as the json mode of the column wants it
local function document(column, text)
  if column.json == 'string' then
    return text
  end
  return json.decode(text)
end

-- A UUID as the uuid mode of the column wants it, from a uuid object
local function identity(column, value)
  if column.uuid == 'string' then
    return value:str()
  end
  if column.uuid == 'binary' then
    return binary(value:bin('b'))
  end
  return value
end

-- A UUID from its text
local function identifier(column, text)
  local value = uuid.fromstr(text)
  if value == nil then
    error(errors.failure('bad UUID ' .. text), 0)
  end
  return identity(column, value)
end

-- A column of a source table: name, type (as the source names it), kind (of
-- the conversion), supported, and the datetime, json and uuid modes from config
local function new(name, data_type, kind, config)
  return {
    name      = name,
    type      = data_type,
    kind      = kind or 'bytes',
    supported = kind ~= nil,
    datetime  = config and config.datetime or 'datetime',
    json      = config and config.json or 'map',
    uuid      = config and config.uuid or 'uuid'
  }
end

return {
  NULL = NULL,
  binary = binary,
  integer = integer,
  float = float,
  moment = moment,
  instant = instant,
  duration = duration,
  clock = clock,
  temporal = temporal,
  document = document,
  identity = identity,
  identifier = identifier,
  new = new
}
