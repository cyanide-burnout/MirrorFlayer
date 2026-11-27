-- Column values of MySQL and MariaDB: one conversion table for the text
-- result set of the full load and for binlog row images, so both paths give
-- the same values. The values themselves are made by MirrorFlayer.Column.

local ffi     = require('ffi')
local bit     = require('bit')
local uuid    = require('uuid')
local decimal = require('decimal')

local protocol = require('MirrorFlayer.MySQL.Protocol')
local convert  = require('MirrorFlayer.Column')
local JSON     = require('MirrorFlayer.MySQL.JSON')

local band, bor, bxor, lshift = bit.band, bit.bor, bit.bxor, bit.lshift
local byte, sub, format = string.byte, string.sub, string.format

local NULL = box.NULL

local binary, moment, instant, duration = convert.binary, convert.moment, convert.instant, convert.duration

-- Wire types of the binlog
local T = {
  DECIMAL = 0, TINY = 1, SHORT = 2, LONG = 3, FLOAT = 4, DOUBLE = 5, NULL = 6,
  TIMESTAMP = 7, LONGLONG = 8, INT24 = 9, DATE = 10, TIME = 11, DATETIME = 12,
  YEAR = 13, NEWDATE = 14, VARCHAR = 15, BIT = 16, TIMESTAMP2 = 17,
  DATETIME2 = 18, TIME2 = 19, JSON = 245, NEWDECIMAL = 246, ENUM = 247,
  SET = 248, TINY_BLOB = 249, MEDIUM_BLOB = 250, LONG_BLOB = 251, BLOB = 252,
  VAR_STRING = 253, STRING = 254, GEOMETRY = 255
}

-- Size of the type metadata in TABLE_MAP_EVENT
local META = {
  [T.FLOAT] = 1, [T.DOUBLE] = 1, [T.BLOB] = 1, [T.GEOMETRY] = 1, [T.JSON] = 1,
  [T.TIMESTAMP2] = 1, [T.DATETIME2] = 1, [T.TIME2] = 1,
  [T.VARCHAR] = 2, [T.VAR_STRING] = 2, [T.BIT] = 2, [T.NEWDECIMAL] = 2,
  [T.STRING] = 2, [T.ENUM] = 2, [T.SET] = 2
}

-- Kinds of values, by DATA_TYPE of information_schema.COLUMNS
local KINDS = {
  tinyint = 'integer', smallint = 'integer', mediumint = 'integer', int = 'integer',
  integer = 'integer', bigint = 'integer',
  float = 'float', double = 'double', real = 'double',
  decimal = 'decimal', numeric = 'decimal',
  date = 'date', datetime = 'datetime', timestamp = 'datetime', time = 'time', year = 'year',
  char = 'string', varchar = 'string', tinytext = 'string', text = 'string',
  mediumtext = 'string', longtext = 'string', json = 'json',
  binary = 'bytes', varbinary = 'bytes', tinyblob = 'bytes', blob = 'bytes',
  mediumblob = 'bytes', longblob = 'bytes',
  bit = 'bit', enum = 'enum', set = 'set', uuid = 'uuid'
}

-- Values -----------------------------------------------------------------------

local function bits(data)
  local value = 0
  for index = 1, #data do
    value = value * 0x100 + byte(data, index)
  end
  return value
end

local function members(column, mask)
  local result = {}
  for index, name in ipairs(column.values) do
    if mask % 2 == 1 then
      table.insert(result, name)
    end
    mask = math.floor(mask / 2)
  end
  return table.concat(result, ',')
end

-- Text result set ------------------------------------------------------------

local TEXT = {
  integer  = function (column, text) return convert.integer(text) end,
  float    = function (column, text) return convert.float(tonumber(text)) end,
  double   = function (column, text) return tonumber(text) end,
  decimal  = function (column, text) return decimal.new(text) end,
  date     = convert.temporal,
  datetime = convert.temporal,
  time     = convert.clock,
  year     = function (column, text) return tonumber(text) end,
  string   = function (column, text) return text end,
  bytes    = function (column, text) return binary(text) end,
  bit      = function (column, text) return bits(text) end,
  enum     = function (column, text) return text end,
  set      = function (column, text) return text end,
  uuid     = convert.identifier,
  json     = convert.document
}

-- Converts a text value of the column; text is a string or box.NULL
local function text(column, value)
  if value == NULL then
    return NULL
  end
  return TEXT[column.kind](column, value)
end

-- Binlog row image -----------------------------------------------------------

-- Reads the type metadata of TABLE_MAP_EVENT for one column
local function meta(data, type)
  local size = META[type]
  if size == nil then
    return 0
  end
  if size == 1 then
    return data:u8()
  end
  if type == T.VARCHAR or type == T.VAR_STRING then
    return data:u16()
  end
  -- Two separate bytes: (bits, bytes), (precision, scale), (real type, length)
  local first = data:u8()
  return { first, data:u8() }
end

local DIGITS = { [0] = 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 }

-- Packed DECIMAL: groups of nine digits in four bytes, big-endian, sign in
-- the high bit of the first byte, negative values inverted
-- The text of a binary DECIMAL of precision and scale
local function decimal_text(data, precision, scale)
  local whole = precision - scale
  local whole9, wholex = math.floor(whole / 9), whole % 9
  local frac9, fracx = math.floor(scale / 9), scale % 9
  local size     = whole9 * 4 + DIGITS[wholex] + frac9 * 4 + DIGITS[fracx]
  local raw      = data:bytes(size)
  local negative = band(byte(raw, 1), 0x80) == 0
  local mask     = negative and 0xff or 0
  local buffer   = {}
  local stream, parts, value

  for index = 1, size do
    local octet = byte(raw, index)
    if index == 1 then
      octet = bxor(octet, 0x80)
    end
    buffer[index] = string.char(bxor(octet, mask))
  end
  stream = protocol.reader(table.concat(buffer))

  parts        = { negative and '-' or '' }
  value        = DIGITS[wholex] > 0 and stream:be(DIGITS[wholex]) or 0
  local digits = tostring(value)
  for _ = 1, whole9 do
    digits = digits .. format('%09d', stream:be(4))
  end
  digits = string.gsub(digits, '^0+', '')
  table.insert(parts, digits ~= '' and digits or '0')
  if scale > 0 then
    table.insert(parts, '.')
    for _ = 1, frac9 do
      table.insert(parts, format('%09d', stream:be(4)))
    end
    if fracx > 0 then
      table.insert(parts, format('%0' .. fracx .. 'd', stream:be(DIGITS[fracx])))
    end
  end
  return table.concat(parts)
end

local function newdecimal(data, precision, scale)
  return decimal.new(decimal_text(data, precision, scale))
end

-- Microseconds of DATETIME2, TIMESTAMP2 and TIME2: (precision + 1) / 2 bytes
local function fraction(data, precision)
  local size = math.floor((precision + 1) / 2)
  if size == 0 then
    return 0
  end
  return data:be(size) * 10 ^ (6 - 2 * size)
end

-- TIME2: the packed value (seconds part << 24 | microseconds) as MySQL keeps
-- it, three bytes and the fraction, or six bytes together for 5-6 digits;
-- returns negative, hours, minutes, seconds, microseconds
local function time2(data, precision)
  local size = math.floor((precision + 1) / 2)
  local packed
  if size == 3 then
    packed = data:be(6) - 0x800000000000
  else
    local whole = data:be(3) - 0x800000
    local part  = size == 1 and data:u8() or size == 2 and data:be(2) or 0
    if whole < 0 and part ~= 0 then
      whole = whole + 1
      part  = part - (size == 1 and 0x100 or 0x10000)
    end
    packed = whole * 0x1000000 + part * (size == 1 and 10000 or size == 2 and 100 or 0)
  end
  local negative = packed < 0
  packed         = math.abs(packed)
  local whole    = math.floor(packed / 0x1000000)
  return negative, math.floor(whole / 0x1000) % 0x400, math.floor(whole / 64) % 64, whole % 64, packed % 0x1000000
end

local function length_prefixed(data, size)
  local length
  if size == 1 then length = data:u8()
  elseif size == 2 then length = data:u16()
  elseif size == 3 then length = data:u24()
  else length = data:u32() end
  return data:bytes(length)
end

-- MySQL binary JSON ------------------------------------------------------------

-- The binlog of MySQL keeps JSON in its binary form: MirrorFlayer.MySQL.JSON
-- makes it the Lua values of the map mode (maps and arrays marked for
-- MsgPack, JSON null as box.NULL) or the text the text protocol prints

-- MariaDB keeps a UUID of version 1-5 and the RFC 4122 variant in its row
-- with the five groups in reverse order, others as they are
local function uuid_row(column, value)
  local swapped = sub(value, 13, 16) .. sub(value, 11, 12) .. sub(value, 9, 10) .. sub(value, 7, 8) .. sub(value, 1, 6)
  local version = math.floor(byte(swapped, 7) / 16)
  if version >= 1 and version <= 5 and band(byte(swapped, 9), 0xc0) == 0x80 then
    value = swapped
  end
  return convert.identity(column, uuid.frombin(value, 'b'))
end

local function finish(column, value)
  if column.kind == 'bytes' then
    return binary(value)
  end
  if column.kind == 'uuid' then
    return uuid_row(column, value)
  end
  if column.kind == 'json' then
    return convert.document(column, value)
  end
  return value
end

local function enumeration(column, value)
  if column.kind == 'set' then
    return members(column, value)
  end
  if value == 0 then
    return ''
  end
  return column.values[value] or ''
end

-- Reads a non-NULL value of the column from a row image.
-- type and info are the wire type and metadata from TABLE_MAP_EVENT.
local function decode(data, column, type, info)
  local value, high

  if type == T.TINY then
    return column.unsigned and data:u8() or data:int(1)
  elseif type == T.SHORT then
    return column.unsigned and data:u16() or data:int(2)
  elseif type == T.INT24 then
    return column.unsigned and data:u24() or data:int(3)
  elseif type == T.LONG then
    return column.unsigned and data:u32() or data:int(4)
  elseif type == T.LONGLONG then
    return column.unsigned and data:u64() or data:i64()
  elseif type == T.FLOAT then
    return data:float()
  elseif type == T.DOUBLE then
    return data:double()
  elseif type == T.NEWDECIMAL then
    return newdecimal(data, info[1], info[2])
  elseif type == T.YEAR then
    value = data:u8()
    return value == 0 and 0 or value + 1900
  elseif type == T.DATE or type == T.NEWDATE then
    value = data:u24()
    return moment(column, math.floor(value / 512), math.floor(value / 32) % 16, value % 32, 0, 0, 0, 0)
  elseif type == T.DATETIME then
    value = data:u64()
    if value == 0 then
      return NULL
    end
    local day, time = math.floor(value / 1000000), value % 1000000
    return moment(column, math.floor(day / 10000), math.floor(day / 100) % 100, day % 100,
      math.floor(time / 10000), math.floor(time / 100) % 100, time % 100, 0)
  elseif type == T.DATETIME2 then
    value      = data:be(5) - 0x8000000000
    local usec = fraction(data, info)
    local ymd, hms = math.floor(value / 0x20000), value % 0x20000
    local ym = math.floor(ymd / 32)
    return moment(column, math.floor(ym / 13), ym % 13, ymd % 32,
      math.floor(hms / 0x1000), math.floor(hms / 64) % 64, hms % 64, usec)
  elseif type == T.TIMESTAMP then
    return instant(column, data:u32(), 0)
  elseif type == T.TIMESTAMP2 then
    value = data:be(4)
    return instant(column, value, fraction(data, info))
  elseif type == T.TIME then
    value          = data:int(3)
    local negative = value < 0
    value          = math.abs(value)
    return duration(column, negative, math.floor(value / 10000), math.floor(value / 100) % 100, value % 100, 0)
  elseif type == T.TIME2 then
    return duration(column, time2(data, info))
  elseif type == T.VARCHAR or type == T.VAR_STRING then
    return finish(column, length_prefixed(data, info > 255 and 2 or 1))
  elseif type == T.BLOB or type == T.GEOMETRY then
    return finish(column, length_prefixed(data, info))
  elseif type == T.BIT then
    return bits(data:bytes(info[2] + (info[1] > 0 and 1 or 0)))
  elseif type == T.STRING or type == T.ENUM or type == T.SET then
    local real, length = info[1], info[2]
    if band(real, 0x30) ~= 0x30 then
      length = bor(length, lshift(bxor(band(real, 0x30), 0x30), 4))
      real   = bor(real, 0x30)
    end
    if real == T.ENUM or real == T.SET then
      high = length
      if high <= 4 then
        value = high == 1 and data:u8() or high == 2 and data:u16() or high == 3 and data:u24() or data:u32()
      else
        value         = data:u32() + data:u32() * 0x100000000
        data.position = data.position - 8 + high
      end
      return enumeration(column, value)
    end
    return finish(column, length_prefixed(data, length > 255 and 2 or 1))
  elseif type == T.JSON then
    return JSON.render(JSON.from_binary(length_prefixed(data, info)), column.json)
  elseif type == T.NULL then
    return NULL
  end

  protocol.fatal('column %s: binlog type %d is not supported', column.name, type)
end

-- Columns --------------------------------------------------------------------

-- Parses the member list of enum('a','b') / set('a','b')
local function values(definition)
  local result   = {}
  local position = string.find(definition, '(', 1, true)
  if position == nil then
    return result
  end
  position = position + 1
  while true do
    local start = string.find(definition, "'", position, true)
    if start == nil then
      break
    end
    local buffer = {}
    local index  = start + 1
    while index <= #definition do
      local symbol = sub(definition, index, index)
      if symbol == "'" then
        if sub(definition, index + 1, index + 1) == "'" then
          table.insert(buffer, "'")
          index = index + 2
        else
          break
        end
      elseif symbol == '\\' then
        table.insert(buffer, sub(definition, index + 1, index + 1))
        index = index + 2
      else
        table.insert(buffer, symbol)
        index = index + 1
      end
    end
    table.insert(result, table.concat(buffer))
    position = index + 1
  end
  return result
end

-- Builds a column from a row of information_schema.COLUMNS:
-- COLUMN_NAME, DATA_TYPE, COLUMN_TYPE; config gives the datetime and json modes
local function describe(name, data_type, column_type, config)
  local kind      = KINDS[string.lower(data_type)]
  local column    = convert.new(name, string.lower(data_type), kind, config)
  column.unsigned = string.find(string.lower(column_type), 'unsigned', 1, true) ~= nil
  if kind == 'enum' or kind == 'set' then
    column.values = values(column_type)
  end
  return column
end

return {
  T        = T,
  decimal  = decimal_text,
  meta     = meta,
  text     = text,
  decode   = decode,
  describe = describe
}
