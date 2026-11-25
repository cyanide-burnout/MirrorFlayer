-- Column values of PostgreSQL: types by OID, text values of SELECT and of
-- pgoutput (the same output functions print both), made by
-- MirrorFlayer.Column. The session prints dates in ISO with TimeZone UTC,
-- intervals in ISO 8601 and bytea in hex.

local decimal  = require('decimal')
local datetime = require('datetime')

local convert = require('MirrorFlayer.Column')
local errors  = require('MirrorFlayer.Failure')

local NULL = box.NULL

-- Kinds by type OID; a domain counts as its base type
local OIDS = {
  [16] = 'boolean',
  [20] = 'integer', [21] = 'integer', [23] = 'integer', [26] = 'integer',
  [700] = 'float', [701] = 'double', [1700] = 'decimal',
  [18] = 'string', [19] = 'string', [25] = 'string', [142] = 'string', [650] = 'string', [829] = 'string',
  [774] = 'string', [869] = 'string', [1042] = 'string', [1043] = 'string',
  [17] = 'bytes',
  [2950] = 'uuid',
  [114] = 'json', [3802] = 'json',
  [1082] = 'date', [1114] = 'datetime', [1184] = 'datetime',
  [1083] = 'time', [1186] = 'interval'
}

local SPECIAL = { ['NaN'] = 0 / 0, ['Infinity'] = math.huge, ['-Infinity'] = -math.huge }

local function moment(column, text)
  if text == 'infinity' or text == '-infinity' then
    return NULL
  end
  if string.find(text, ' BC', 1, true) ~= nil then
    error(errors.failure('dates BC are not supported: ' .. text), 0)
  end
  return convert.temporal(column, text)
end

local SECONDS = { Y = 365.25 * 86400, M = 30 * 86400, W = 7 * 86400, D = 86400 }
local FIELDS  = { Y = 'year', M = 'month', W = 'week', D = 'day' }
local CLOCK   = { H = 'hour', M = 'min', S = 'sec' }
local UNITS   = { H = 3600, M = 60, S = 1 }

-- An interval in ISO 8601 (P1Y2M3DT4H5M6.5S, every part with its sign):
-- interval of Tarantool, or seconds in the number mode, a month being 30
-- days and a year 365.25 days as EXTRACT(EPOCH) counts them
local function interval(column, text)
  local date, time = string.match(text, '^P([^T]*)T?(.*)$')
  if date == nil then
    error(errors.failure('bad interval ' .. text), 0)
  end
  local fields, seconds = {}, 0
  for value, unit in string.gmatch(date, '(%-?%d+)([YMWD])') do
    fields[FIELDS[unit]] = tonumber(value)
    seconds = seconds + tonumber(value) * SECONDS[unit]
  end
  for value, unit in string.gmatch(time, '(%-?[%d%.]+)([HMS])') do
    local number = tonumber(value)
    seconds = seconds + number * UNITS[unit]
    if unit == 'S' then
      local whole = number < 0 and -math.floor(-number) or math.floor(number)
      fields.sec  = whole
      fields.nsec = math.floor((number - whole) * 1e9 + (number < 0 and -0.5 or 0.5))
    else
      fields[CLOCK[unit]] = number
    end
  end
  if column.datetime == 'number' then
    return seconds
  end
  return datetime.interval.new(fields)
end

local function hex(text)
  if string.sub(text, 1, 2) ~= '\\x' then
    error(errors.failure('bytea is not in hex'), 0)
  end
  return convert.binary((string.gsub(string.sub(text, 3), '%x%x', function (pair)
    return string.char(tonumber(pair, 16))
  end)))
end

local TEXT = {
  boolean  = function (column, text) return text == 't' end,
  integer  = function (column, text) return convert.integer(text) end,
  float    = function (column, text) return SPECIAL[text] or convert.float(tonumber(text)) end,
  double   = function (column, text) return SPECIAL[text] or tonumber(text) end,
  decimal  = function (column, text)
    if SPECIAL[text] ~= nil then
      return NULL
    end
    return decimal.new(text)
  end,
  string   = function (column, text) return text end,
  bytes    = function (column, text) return hex(text) end,
  uuid     = convert.identifier,
  json     = convert.document,
  date     = moment,
  datetime = moment,
  time     = convert.clock,
  interval = interval
}

-- Converts a text value of the column; text is a string or box.NULL
local function text(column, value)
  if value == NULL then
    return NULL
  end
  return TEXT[column.kind](column, value)
end

-- Builds a column from the catalogue: name, type OID of the column (what
-- pgoutput names), OID, name, typtype and typcategory of its base type (the
-- type itself unless it is a domain); enums and other string types are
-- strings, the rest is refused when mapped
local function describe(name, oid, base, type_name, type_type, category, config)
  local kind = OIDS[tonumber(base)]
  if kind == nil and (type_type == 'e' or category == 'S') then
    kind = 'string'
  end
  local column = convert.new(name, type_name, kind, config)
  column.oid = tonumber(oid)
  return column
end

return {
  text = text,
  describe = describe
}
