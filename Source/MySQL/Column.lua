-- Column values of MySQL and MariaDB: one conversion table for the text
-- result set of the full load and for binlog row images, so both paths give
-- the same values. The values themselves are made by MirrorFlayer.Column.

local ffi      = require('ffi')
local bit      = require('bit')
local uuid     = require('uuid')
local decimal  = require('decimal')

local protocol = require('MirrorFlayer.MySQL.Protocol')
local convert  = require('MirrorFlayer.Column')

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
local function newdecimal(data, precision, scale)
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

  parts = { negative and '-' or '' }
  value = DIGITS[wholex] > 0 and stream:be(DIGITS[wholex]) or 0
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
  return decimal.new(table.concat(parts))
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
  packed = math.abs(packed)
  local whole = math.floor(packed / 0x1000000)
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

-- The binlog of MySQL keeps JSON in its binary form; it becomes the same Lua
-- values json.decode() gives for the text protocol: maps and arrays marked for
-- MsgPack, JSON null as box.NULL

local J = {
  SMALL_OBJECT = 0x0, LARGE_OBJECT = 0x1, SMALL_ARRAY = 0x2, LARGE_ARRAY = 0x3,
  LITERAL = 0x4, INT16 = 0x5, UINT16 = 0x6, INT32 = 0x7, UINT32 = 0x8,
  INT64 = 0x9, UINT64 = 0xa, DOUBLE = 0xb, STRING = 0xc, OPAQUE = 0xf
}

local MAP   = { __serialize = 'map' }
local ARRAY = { __serialize = 'array' }

local function literal(value)
  if value == 1 then
    return true
  end
  if value == 2 then
    return false
  end
  return NULL
end

local json_value

local function json_varlen(data)
  local value, shift = 0, 1
  for _ = 1, 5 do
    local octet = data:u8()
    value = value + (octet % 0x80) * shift
    if octet < 0x80 then
      return value
    end
    shift = shift * 0x80
  end
  protocol.fatal('bad length in binary JSON')
end

-- A value entry of an object or array: inlined or at an offset from base
local function json_entry(data, base, large)
  local kind = data:u8()
  local value
  if kind == J.LITERAL then
    return literal(large and data:u32() or data:u16())
  end
  if kind == J.INT16 then
    value = data:int(2)
    if large then data:skip(2) end
    return value
  end
  if kind == J.UINT16 then
    value = data:u16()
    if large then data:skip(2) end
    return value
  end
  if large and kind == J.INT32 then
    return data:int(4)
  end
  if large and kind == J.UINT32 then
    return data:u32()
  end
  local offset = large and data:u32() or data:u16()
  local saved  = data.position
  data.position = base + offset
  value = json_value(data, kind)
  data.position = saved
  return value
end

local function json_container(data, kind)
  local base   = data.position
  local large  = kind == J.LARGE_OBJECT or kind == J.LARGE_ARRAY
  local object = kind == J.SMALL_OBJECT or kind == J.LARGE_OBJECT
  local count  = large and data:u32() or data:u16()
  local keys   = {}
  local result = setmetatable({}, object and MAP or ARRAY)
  data:skip(large and 4 or 2) -- size
  if object then
    for index = 1, count do
      local offset = large and data:u32() or data:u16()
      local length = data:u16()
      keys[index] = sub(data.data, base + offset, base + offset + length - 1)
    end
  end
  for index = 1, count do
    result[object and keys[index] or index] = json_entry(data, base, large)
  end
  return result
end

json_value = function (data, kind)
  if kind <= J.LARGE_ARRAY then
    return json_container(data, kind)
  elseif kind == J.LITERAL then
    return literal(data:u8())
  elseif kind == J.INT16 then
    return data:int(2)
  elseif kind == J.UINT16 then
    return data:u16()
  elseif kind == J.INT32 then
    return data:int(4)
  elseif kind == J.UINT32 then
    return data:u32()
  elseif kind == J.INT64 then
    return data:i64()
  elseif kind == J.UINT64 then
    return data:u64()
  elseif kind == J.DOUBLE then
    return data:double()
  elseif kind == J.STRING then
    return data:bytes(json_varlen(data))
  end
  protocol.fatal('binary JSON value of type %d (opaque: DECIMAL, dates inside JSON) is not supported', kind)
end

local function binary_json(raw)
  if #raw == 0 then
    return NULL
  end
  local data = protocol.reader(raw)
  return json_value(data, data:u8())
end

-- In the string mode the binary JSON of MySQL is printed back the way the
-- text protocol prints it: '{"a": 1, "b": [true, null]}', keys in the stored
-- order, so the full load and the stream give the same string

local LITERALS = { [0] = 'null', 'true', 'false' }

local ESCAPES = {
  ['"'] = '\\"', ['\\'] = '\\\\', ['\b'] = '\\b', ['\f'] = '\\f',
  ['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t'
}

local function quote(text)
  return '"' .. string.gsub(text, '[%z\1-\31"\\]', function (symbol)
    return ESCAPES[symbol] or format('\\u%04x', byte(symbol))
  end) .. '"'
end

local function integer_text(value)
  if type(value) == 'number' then
    return format('%.0f', value)
  end
  return (string.gsub(tostring(value), 'U?LL$', ''))
end

-- Shortest text that reads back as the same double, exponents without '+'
-- and leading zeros, integral values with '.0'
local function double_text(value)
  local text
  for precision = 15, 17 do
    text = format('%.' .. precision .. 'g', value)
    if tonumber(text) == value then
      break
    end
  end
  text = string.gsub(text, 'e%+?(%-?)0*(%d)', 'e%1%2')
  if string.find(text, '[%.eni]') == nil then
    text = text .. '.0'
  end
  return text
end

local text_value

local function text_entry(data, base, large)
  local kind = data:u8()
  local value
  if kind == J.LITERAL then
    value = large and data:u32() or data:u16()
    return LITERALS[value] or 'null'
  end
  if kind == J.INT16 then
    value = data:int(2)
    if large then data:skip(2) end
    return integer_text(value)
  end
  if kind == J.UINT16 then
    value = data:u16()
    if large then data:skip(2) end
    return integer_text(value)
  end
  if large and kind == J.INT32 then
    return integer_text(data:int(4))
  end
  if large and kind == J.UINT32 then
    return integer_text(data:u32())
  end
  local offset = large and data:u32() or data:u16()
  local saved  = data.position
  data.position = base + offset
  value = text_value(data, kind)
  data.position = saved
  return value
end

local function text_container(data, kind)
  local base   = data.position
  local large  = kind == J.LARGE_OBJECT or kind == J.LARGE_ARRAY
  local object = kind == J.SMALL_OBJECT or kind == J.LARGE_OBJECT
  local count  = large and data:u32() or data:u16()
  local keys   = {}
  local items  = {}
  data:skip(large and 4 or 2) -- size
  if object then
    for index = 1, count do
      local offset = large and data:u32() or data:u16()
      local length = data:u16()
      keys[index] = sub(data.data, base + offset, base + offset + length - 1)
    end
  end
  for index = 1, count do
    items[index] = text_entry(data, base, large)
    if object then
      items[index] = quote(keys[index]) .. ': ' .. items[index]
    end
  end
  if object then
    return '{' .. table.concat(items, ', ') .. '}'
  end
  return '[' .. table.concat(items, ', ') .. ']'
end

text_value = function (data, kind)
  if kind <= J.LARGE_ARRAY then
    return text_container(data, kind)
  elseif kind == J.LITERAL then
    return LITERALS[data:u8()] or 'null'
  elseif kind == J.INT16 then
    return integer_text(data:int(2))
  elseif kind == J.UINT16 then
    return integer_text(data:u16())
  elseif kind == J.INT32 then
    return integer_text(data:int(4))
  elseif kind == J.UINT32 then
    return integer_text(data:u32())
  elseif kind == J.INT64 then
    return integer_text(data:i64())
  elseif kind == J.UINT64 then
    return integer_text(data:u64())
  elseif kind == J.DOUBLE then
    return double_text(data:double())
  elseif kind == J.STRING then
    return quote(data:bytes(json_varlen(data)))
  end
  protocol.fatal('binary JSON value of type %d (opaque: DECIMAL, dates inside JSON) is not supported', kind)
end

local function binary_json_text(raw)
  if #raw == 0 then
    return 'null'
  end
  local data = protocol.reader(raw)
  return text_value(data, data:u8())
end


-- MariaDB keeps a UUID of version 1-5 and the RFC 4122 variant in its row
-- with the five groups in reverse order, others as they are
local function uuid_row(value)
  local swapped = sub(value, 13, 16) .. sub(value, 11, 12) .. sub(value, 9, 10) .. sub(value, 7, 8) .. sub(value, 1, 6)
  local version = math.floor(byte(swapped, 7) / 16)
  if version >= 1 and version <= 5 and band(byte(swapped, 9), 0xc0) == 0x80 then
    value = swapped
  end
  return uuid.frombin(value, 'b')
end

local function finish(column, value)
  if column.kind == 'bytes' then
    return binary(value)
  end
  if column.kind == 'uuid' then
    return uuid_row(value)
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
    value = data:be(5) - 0x8000000000
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
    value = data:int(3)
    local negative = value < 0
    value = math.abs(value)
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
      real = bor(real, 0x30)
    end
    if real == T.ENUM or real == T.SET then
      high = length
      if high <= 4 then
        value = high == 1 and data:u8() or high == 2 and data:u16() or high == 3 and data:u24() or data:u32()
      else
        value = data:u32() + data:u32() * 0x100000000
        data.position = data.position - 8 + high
      end
      return enumeration(column, value)
    end
    return finish(column, length_prefixed(data, length > 255 and 2 or 1))
  elseif type == T.JSON then
    if column.json == 'string' then
      return binary_json_text(length_prefixed(data, info))
    end
    return binary_json(length_prefixed(data, info))
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
  local kind   = KINDS[string.lower(data_type)]
  local column = convert.new(name, string.lower(data_type), kind, config)
  column.unsigned = string.find(string.lower(column_type), 'unsigned', 1, true) ~= nil
  if kind == 'enum' or kind == 'set' then
    column.values = values(column_type)
  end
  return column
end

return {
  T = T,
  meta = meta,
  text = text,
  decode = decode,
  describe = describe
}
