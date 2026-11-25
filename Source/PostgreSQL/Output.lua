-- pgoutput, protocol version 1: Begin, Commit, Relation, Insert, Update,
-- Delete, Truncate; Origin, Type and Message are skipped. LSNs are uint64_t,
-- times are seconds since the Unix epoch.

local ffi = require('ffi')

local protocol = require('MirrorFlayer.PostgreSQL.Protocol')
local convert  = require('MirrorFlayer.Column')
local errors   = require('MirrorFlayer.Failure')

local char = string.char

local NULL      = box.NULL
local UNCHANGED = convert.UNCHANGED

-- Seconds between the Unix epoch and 2000-01-01, the epoch of PostgreSQL
local EPOCH = 946684800

local function time(data)
  return data:i64() / 1e6 + EPOCH
end

-- LSNs ---------------------------------------------------------------------------

-- 'X/Y' into uint64_t
local function lsn(text)
  local high, low = string.match(text or '', '^(%x+)/(%x+)$')
  if high == nil then
    errors.fatal('bad LSN %q', tostring(text))
  end
  return ffi.cast('uint64_t', tonumber(high, 16)) * 0x100000000ULL + tonumber(low, 16)
end

local function format(value)
  value = ffi.cast('uint64_t', value)
  return string.format('%X/%X', tonumber(value / 0x100000000ULL), tonumber(value % 0x100000000ULL))
end

-- Messages -----------------------------------------------------------------------

-- TupleData: text values, box.NULL, or UNCHANGED for a TOAST value the
-- update did not touch
local function tuple(data)
  local count  = data:u16()
  local values = {}
  for index = 1, count do
    local kind = char(data:u8())
    if kind == 'n' then
      values[index] = NULL
    elseif kind == 'u' then
      values[index] = UNCHANGED
    elseif kind == 't' or kind == 'b' then
      values[index] = data:bytes(data:i32())
    else
      errors.fatal('pgoutput: tuple value of kind %q', kind)
    end
  end
  return values
end

-- Decodes one message from the payload of XLogData
local function decode(payload)
  local data = protocol.reader(payload)
  local kind = char(data:u8())
  if kind == 'B' then
    return { kind = kind, lsn = data:u64(), time = time(data), xid = data:u32() }
  elseif kind == 'C' then
    data:u8() -- flags
    return { kind = kind, lsn = data:u64(), finish = data:u64(), time = time(data) }
  elseif kind == 'R' then
    local message = { kind = kind, relation = data:u32(), namespace = data:cstring(), name = data:cstring(),
      identity = char(data:u8()), columns = {} }
    for index = 1, data:u16() do
      message.columns[index] = { key = data:u8() == 1, name = data:cstring(), oid = data:u32(), modifier = data:i32() }
    end
    return message
  elseif kind == 'I' then
    local message = { kind = kind, relation = data:u32() }
    data:u8() -- 'N'
    message.new = tuple(data)
    return message
  elseif kind == 'U' then
    local message = { kind = kind, relation = data:u32() }
    local part    = char(data:u8())
    if part == 'K' or part == 'O' then
      message.old  = tuple(data)
      message.full = part == 'O'
      part = char(data:u8())
    end
    message.new = tuple(data)
    return message
  elseif kind == 'D' then
    local message = { kind = kind, relation = data:u32() }
    message.full = char(data:u8()) == 'O'
    message.old  = tuple(data)
    return message
  elseif kind == 'T' then
    local message = { kind = kind, relations = {} }
    local count   = data:u32()
    data:u8() -- options
    for index = 1, count do
      message.relations[index] = data:u32()
    end
    return message
  end
  -- Origin, Type, Message
  return { kind = kind }
end

return {
  EPOCH = EPOCH,
  lsn = lsn,
  format = format,
  decode = decode
}
