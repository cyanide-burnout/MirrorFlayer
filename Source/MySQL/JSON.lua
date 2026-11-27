-- JSON of MySQL as a tree: from the binary form of the binlog, from the text
-- of the string mode or from the Lua values of the map mode, back to either
-- mode, and the partial updates of PARTIAL_UPDATE_ROWS (binlog_row_value_options
-- = PARTIAL_JSON) applied to it.
--
-- A node is { kind, value }: 'object' with keys and values in order, 'array'
-- with items, 'integer' (a number or int64_t/uint64_t cdata), 'double' (with
-- its text when it came as text), 'decimal' (its text), 'string', 'true',
-- 'false', 'null'. Dates and times inside JSON are strings, as MySQL prints
-- them; other opaque values 'base64:typeN:...'. The text is printed the way the text
-- protocol of MySQL prints it, '{"a": 1, "b": [true, null]}', object keys
-- ordered by length and then by bytes, as the binary form of MySQL keeps them.

local ffi = require('ffi')

local protocol = require('MirrorFlayer.MySQL.Protocol')

local byte, char, sub, format = string.byte, string.char, string.sub, string.format

local NULL  = box.NULL
local MAP   = { __serialize = 'map' }
local ARRAY = { __serialize = 'array' }

local J = {
  SMALL_OBJECT = 0x0, LARGE_OBJECT = 0x1, SMALL_ARRAY = 0x2, LARGE_ARRAY = 0x3,
  LITERAL = 0x4, INT16 = 0x5, UINT16 = 0x6, INT32 = 0x7, UINT32 = 0x8,
  INT64 = 0x9, UINT64 = 0xa, DOUBLE = 0xb, STRING = 0xc, OPAQUE = 0xf
}

local LITERALS = { [0] = 'null', 'true', 'false' }

local function node(kind, value, text)
  return { kind = kind, value = value, text = text }
end

-- Binary form ----------------------------------------------------------------

local binary_value

local function varlen_of(data)
  local value, shift = 0, 1
  for _ = 1, 5 do
    local next = data:u8()
    value = value + (next % 0x80) * shift
    if next < 0x80 then
      return value
    end
    shift = shift * 0x80
  end
  protocol.fatal('bad length in binary JSON')
end

-- Field types of opaque values
local NEWDECIMAL, DATE, TIME, DATETIME, TIMESTAMP = 246, 10, 11, 12, 7

local BASE64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'

local function base64(raw)
  local parts = {}
  for index = 1, #raw, 3 do
    local a, b, c = byte(raw, index, index + 2)
    local value = a * 0x10000 + (b or 0) * 0x100 + (c or 0)
    local chunk = {}
    for shift = 3, 0, -1 do
      local digit = math.floor(value / 64 ^ shift) % 64
      table.insert(chunk, sub(BASE64, digit + 1, digit + 1))
    end
    if c == nil then chunk[4] = '=' end
    if b == nil then chunk[3] = '=' end
    table.insert(parts, table.concat(chunk))
  end
  return table.concat(parts)
end

-- A packed TIME, DATE or DATETIME of MySQL: int64 of ((ymd << 17 | hms) <<
-- 24 | microseconds), ymd = (year * 13 + month) << 5 | day, hms = hour << 12
-- | minute << 6 | second; negative for a negative TIME
local function moment(raw, kind)
  local value    = ffi.cast('const int64_t *', raw)[0]
  local negative = value < 0
  if negative then value = -value end
  local micro    = tonumber(value % 0x1000000)
  local ymdhms   = value / 0x1000000
  local hms      = tonumber(ymdhms % 0x20000)
  local ymd      = tonumber(ymdhms / 0x20000)
  local ym       = math.floor(ymd / 32)
  local hour, minute, second = math.floor(hms / 4096), math.floor(hms / 64) % 64, hms % 64
  if kind == TIME then
    -- Hours of a TIME go beyond the 5 bits of a day: they take the bits above
    local whole = tonumber(ymdhms)
    return format('%s%02d:%02d:%02d.%06d', negative and '-' or '', math.floor(whole / 4096), minute, second, micro)
  end
  local date = format('%04d-%02d-%02d', math.floor(ym / 13), ym % 13, ymd % 32)
  if kind == DATE then
    return date
  end
  return format('%s %02d:%02d:%02d.%06d', date, hour, minute, second, micro)
end

-- An opaque value: its field type, the length and the data
local function opaque(data)
  local kind = data:u8()
  local raw  = data:bytes(varlen_of(data))
  if kind == NEWDECIMAL then
    local value = protocol.reader(raw)
    local precision, scale = value:u8(), value:u8()
    return node('decimal', require('MirrorFlayer.MySQL.Column').decimal(value, precision, scale))
  end
  if (kind == DATE or kind == TIME or kind == DATETIME or kind == TIMESTAMP) and #raw >= 8 then
    return node('string', moment(raw, kind))
  end
  return node('string', format('base64:type%d:%s', kind, base64(raw)))
end

local function binary_entry(data, base, large)
  local kind = data:u8()
  if kind == J.LITERAL then
    local value = large and data:u32() or data:u16()
    return node(LITERALS[value] or 'null')
  end
  if kind == J.INT16 then
    local value = data:int(2)
    if large then data:skip(2) end
    return node('integer', value)
  end
  if kind == J.UINT16 then
    local value = data:u16()
    if large then data:skip(2) end
    return node('integer', value)
  end
  if large and kind == J.INT32 then
    return node('integer', data:int(4))
  end
  if large and kind == J.UINT32 then
    return node('integer', data:u32())
  end
  local offset  = large and data:u32() or data:u16()
  local saved   = data.position
  data.position = base + offset
  local value   = binary_value(data, kind)
  data.position = saved
  return value
end

local function binary_container(data, kind)
  local base   = data.position
  local large  = kind == J.LARGE_OBJECT or kind == J.LARGE_ARRAY
  local object = kind == J.SMALL_OBJECT or kind == J.LARGE_OBJECT
  local count  = large and data:u32() or data:u16()
  local keys, values = {}, {}
  data:skip(large and 4 or 2) -- size
  if object then
    for index = 1, count do
      local offset = large and data:u32() or data:u16()
      local length = data:u16()
      keys[index]  = sub(data.data, base + offset, base + offset + length - 1)
    end
  end
  for index = 1, count do
    values[index] = binary_entry(data, base, large)
  end
  if object then
    return node('object', { keys = keys, values = values })
  end
  return node('array', values)
end

binary_value = function (data, kind)
  if kind <= J.LARGE_ARRAY then
    return binary_container(data, kind)
  elseif kind == J.LITERAL then
    return node(LITERALS[data:u8()] or 'null')
  elseif kind == J.INT16 then
    return node('integer', data:int(2))
  elseif kind == J.UINT16 then
    return node('integer', data:u16())
  elseif kind == J.INT32 then
    return node('integer', data:int(4))
  elseif kind == J.UINT32 then
    return node('integer', data:u32())
  elseif kind == J.INT64 then
    return node('integer', data:i64())
  elseif kind == J.UINT64 then
    return node('integer', data:u64())
  elseif kind == J.DOUBLE then
    return node('double', data:double())
  elseif kind == J.STRING then
    return node('string', data:bytes(varlen_of(data)))
  elseif kind == J.OPAQUE then
    return opaque(data)
  end
  protocol.fatal('binary JSON value of type %d is not supported', kind)
end

-- The tree of binary JSON; an empty value is JSON null
local function from_binary(raw)
  if #raw == 0 then
    return node('null')
  end
  local data = protocol.reader(raw)
  return binary_value(data, data:u8())
end

-- Text -----------------------------------------------------------------------

local UNESCAPES = { ['"'] = '"', ['\\'] = '\\', ['/'] = '/', b = '\b', f = '\f', n = '\n', r = '\r', t = '\t' }

-- UTF-8 of a code point
local function utf8(code)
  if code < 0x80 then
    return char(code)
  elseif code < 0x800 then
    return char(0xc0 + math.floor(code / 0x40), 0x80 + code % 0x40)
  elseif code < 0x10000 then
    return char(0xe0 + math.floor(code / 0x1000), 0x80 + math.floor(code / 0x40) % 0x40, 0x80 + code % 0x40)
  end
  return char(0xf0 + math.floor(code / 0x40000), 0x80 + math.floor(code / 0x1000) % 0x40,
    0x80 + math.floor(code / 0x40) % 0x40, 0x80 + code % 0x40)
end

-- Integers as the binary form keeps them: numbers, cdata beyond 2^53
local function integer(text)
  local value = tonumber(text)
  if value ~= nil and math.abs(value) < 2 ^ 53 then
    return value
  end
  if string.sub(text, 1, 1) == '-' then
    return tonumber64(text)
  end
  return ffi.new('uint64_t', tonumber64(text))
end

local text_value

local function skip(text, position)
  return string.find(text, '[^ \t\r\n]', position) or #text + 1
end

local function text_string(text, position)
  local parts = {}
  position = position + 1
  while true do
    local stop = string.find(text, '["\\]', position)
    if stop == nil then
      protocol.fatal('JSON text: string without its end')
    end
    table.insert(parts, sub(text, position, stop - 1))
    if sub(text, stop, stop) == '"' then
      return table.concat(parts), stop + 1
    end
    local escape = sub(text, stop + 1, stop + 1)
    if escape == 'u' then
      local code = tonumber(sub(text, stop + 2, stop + 5), 16)
      position = stop + 6
      if code >= 0xd800 and code < 0xdc00 and sub(text, position, position + 1) == '\\u' then
        local low = tonumber(sub(text, position + 2, position + 5), 16)
        code     = 0x10000 + (code - 0xd800) * 0x400 + (low - 0xdc00)
        position = position + 6
      end
      table.insert(parts, utf8(code))
    else
      table.insert(parts, UNESCAPES[escape] or escape)
      position = stop + 2
    end
  end
end

text_value = function (text, position)
  position = skip(text, position)
  local first = sub(text, position, position)
  if first == '{' then
    local keys, values = {}, {}
    position = skip(text, position + 1)
    if sub(text, position, position) == '}' then
      return node('object', { keys = keys, values = values }), position + 1
    end
    while true do
      local key, value
      key, position = text_string(text, skip(text, position))
      position = skip(text, position) + 1 -- ':'
      value, position = text_value(text, position)
      table.insert(keys, key)
      table.insert(values, value)
      position = skip(text, position)
      if sub(text, position, position) == '}' then
        return node('object', { keys = keys, values = values }), position + 1
      end
      position = position + 1 -- ','
    end
  elseif first == '[' then
    local items = {}
    position = skip(text, position + 1)
    if sub(text, position, position) == ']' then
      return node('array', items), position + 1
    end
    while true do
      local value
      value, position = text_value(text, position)
      table.insert(items, value)
      position = skip(text, position)
      if sub(text, position, position) == ']' then
        return node('array', items), position + 1
      end
      position = position + 1 -- ','
    end
  elseif first == '"' then
    local value
    value, position = text_string(text, position)
    return node('string', value), position
  elseif sub(text, position, position + 3) == 'true' then
    return node('true'), position + 4
  elseif sub(text, position, position + 4) == 'false' then
    return node('false'), position + 5
  elseif sub(text, position, position + 3) == 'null' then
    return node('null'), position + 4
  end
  local number = string.match(text, '^-?%d+%.?%d*[eE]?[-+]?%d*', position)
  if number == nil or number == '' then
    protocol.fatal('JSON text: unexpected %q', sub(text, position, position + 10))
  end
  if string.find(number, '[.eE]') then
    return node('double', tonumber(number), number), position + #number
  end
  return node('integer', integer(number)), position + #number
end

local function from_text(text)
  return (text_value(text, 1))
end

-- Lua values of the map mode -------------------------------------------------

local function from_lua(value)
  if value == nil then -- box.NULL too
    return node('null')
  end
  local kind = type(value)
  if kind == 'boolean' then
    return node(value and 'true' or 'false')
  elseif kind == 'string' then
    return node('string', value)
  elseif kind == 'cdata' then
    return node('integer', value)
  elseif kind == 'number' then
    if value == math.floor(value) and math.abs(value) < 2 ^ 53 then
      return node('integer', value)
    end
    return node('double', value)
  elseif kind == 'table' then
    local meta = getmetatable(value)
    if (meta ~= nil and meta.__serialize == 'map') or (#value == 0 and next(value) ~= nil) then
      local keys, values = {}, {}
      for key, item in pairs(value) do
        table.insert(keys, tostring(key))
        table.insert(values, from_lua(item))
      end
      return node('object', { keys = keys, values = values })
    end
    local items = {}
    for index = 1, #value do
      items[index] = from_lua(value[index])
    end
    return node('array', items)
  end
  protocol.fatal('a JSON value cannot be made of a %s', kind)
end

-- Rendering ------------------------------------------------------------------

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

-- Keys by length and then by bytes, the order of the binary form of MySQL
local function ordered(object)
  local order = {}
  for index = 1, #object.keys do
    order[index] = index
  end
  table.sort(order, function (left, right)
    local a, b = object.keys[left], object.keys[right]
    if #a ~= #b then
      return #a < #b
    end
    return a < b
  end)
  return order
end

local function to_text(tree)
  local kind = tree.kind
  if kind == 'object' then
    local items = {}
    for _, index in ipairs(ordered(tree.value)) do
      table.insert(items, quote(tree.value.keys[index]) .. ': ' .. to_text(tree.value.values[index]))
    end
    return '{' .. table.concat(items, ', ') .. '}'
  elseif kind == 'array' then
    local items = {}
    for index, item in ipairs(tree.value) do
      items[index] = to_text(item)
    end
    return '[' .. table.concat(items, ', ') .. ']'
  elseif kind == 'integer' then
    return integer_text(tree.value)
  elseif kind == 'double' then
    return tree.text or double_text(tree.value)
  elseif kind == 'decimal' then
    return tree.value
  elseif kind == 'string' then
    return quote(tree.value)
  end
  return kind -- true, false, null
end

local function to_lua(tree)
  local kind = tree.kind
  if kind == 'object' then
    local result = setmetatable({}, MAP)
    for index, key in ipairs(tree.value.keys) do
      result[key] = to_lua(tree.value.values[index])
    end
    return result
  elseif kind == 'array' then
    local result = setmetatable({}, ARRAY)
    for index, item in ipairs(tree.value) do
      result[index] = to_lua(item)
    end
    return result
  elseif kind == 'true' then
    return true
  elseif kind == 'false' then
    return false
  elseif kind == 'null' then
    return NULL
  elseif kind == 'decimal' then
    return tonumber(tree.value) -- as json.decode() of the text gives it
  end
  return tree.value
end

-- The value of the mode: 'string' or 'map'
local function render(tree, mode)
  if mode == 'string' then
    return to_text(tree)
  end
  return to_lua(tree)
end

-- Paths and partial updates --------------------------------------------------

-- A path of MySQL as legs: { key = name } or { index = n } or { last = n }
-- ([last] or [last-n]); only member and array legs, as partial updates have
local function path(text)
  local legs, position = {}, 2
  if sub(text, 1, 1) ~= '$' then
    protocol.fatal('JSON path %q does not start with $', text)
  end
  while position <= #text do
    local first = sub(text, position, position)
    if first == '.' then
      if sub(text, position + 1, position + 1) == '"' then
        local key
        key, position = text_string(text, position + 1)
        table.insert(legs, { key = key })
      else
        local key = string.match(text, '^[^.%[]+', position + 1)
        table.insert(legs, { key = key })
        position = position + 1 + #key
      end
    elseif first == '[' then
      local inner = string.match(text, '^%[%s*([^%]]-)%s*%]', position)
      local index = tonumber(inner)
      if index ~= nil then
        table.insert(legs, { index = index })
      else
        local offset = string.match(inner, '^last%s*%-?%s*(%d*)$')
        if offset == nil then
          protocol.fatal('JSON path %q is not supported', text)
        end
        table.insert(legs, { last = tonumber(offset) or 0 })
      end
      position = position + #string.match(text, '^%[[^%]]*%]', position)
    else
      protocol.fatal('JSON path %q is not supported', text)
    end
  end
  return legs
end

local function find(object, key)
  for index, name in ipairs(object.keys) do
    if name == key then
      return index
    end
  end
end

-- Position of an array leg, 1-based
local function position_of(items, leg)
  if leg.last ~= nil then
    return #items - leg.last
  end
  return leg.index + 1
end

-- The node a leg points to inside container, or nil
local function step(container, leg)
  if container.kind == 'object' and leg.key ~= nil then
    local index = find(container.value, leg.key)
    return index and container.value.values[index]
  end
  if container.kind == 'array' and leg.key == nil then
    return container.value[position_of(container.value, leg)]
  end
end

-- Operations of a diff: 0 REPLACE, 1 INSERT, 2 REMOVE
local REPLACE, INSERT, REMOVE = 0, 1, 2

local function apply(tree, operation, legs, value)
  if #legs == 0 then
    if operation == REPLACE then
      return value
    end
    protocol.fatal('partial JSON update of the whole document')
  end
  local parent = tree
  for index = 1, #legs - 1 do
    parent = step(parent, legs[index])
    if parent == nil then
      protocol.fatal('partial JSON update: the path does not exist in the stored document')
    end
  end
  local leg = legs[#legs]
  if parent.kind == 'object' and leg.key ~= nil then
    local object = parent.value
    local index  = find(object, leg.key)
    if operation == REMOVE then
      if index ~= nil then
        table.remove(object.keys, index)
        table.remove(object.values, index)
      end
    elseif index ~= nil then
      object.values[index] = value
    else
      table.insert(object.keys, leg.key)
      table.insert(object.values, value)
    end
  elseif parent.kind == 'array' and leg.key == nil then
    local items    = parent.value
    local position = position_of(items, leg)
    if operation == REMOVE then
      if items[position] ~= nil then
        table.remove(items, position)
      end
    elseif operation == INSERT then
      table.insert(items, math.max(1, math.min(position, #items + 1)), value)
    else
      items[position] = value
    end
  else
    protocol.fatal('partial JSON update: the path does not fit the stored document')
  end
  return tree
end

-- Diffs of a partial update: an operation byte, the path and, but for REMOVE,
-- the value in the binary form, lengths as length-encoded integers
local function diffs(raw)
  local data, result = protocol.reader(raw), {}
  while data:left() > 0 do
    local operation = data:u8()
    local text      = data:bytes(data:lenenc())
    local value
    if operation ~= REMOVE then
      value = data:bytes(data:lenenc())
    end
    table.insert(result, { operation, text, value })
  end
  return result
end

-- Applies the diffs to a stored value of the mode ('string' text or 'map'
-- Lua values); returns the new value of the mode
local function patch(stored, list, mode)
  local tree
  if mode == 'string' then
    tree = type(stored) == 'string' and from_text(stored) or node('null')
  else
    tree = from_lua(stored)
  end
  for _, diff in ipairs(list) do
    tree = apply(tree, diff[1], path(diff[2]), diff[3] ~= nil and from_binary(diff[3]) or nil)
  end
  return render(tree, mode)
end

return {
  from_binary = from_binary,
  from_text   = from_text,
  from_lua    = from_lua,
  render      = render,
  diffs       = diffs,
  patch       = patch
}
