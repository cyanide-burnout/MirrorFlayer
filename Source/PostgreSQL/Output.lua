-- pgoutput, protocol versions 1 and 2: Begin, Commit, Relation, Insert,
-- Update, Delete, Truncate; with version 2 (PostgreSQL 14 and later, streaming
-- 'on') a large transaction comes in blocks while it runs: Stream Start and
-- Stop around them, the changes inside with the xid of their (sub)transaction,
-- Stream Commit and Stream Abort at the end. Origin, Type and Message are
-- skipped. LSNs are uint64_t, times are seconds since the Unix epoch.

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

-- A message of a transaction, its kind read
local function decode_change(kind, data)
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
      part         = char(data:u8())
    end
    message.new = tuple(data)
    return message
  elseif kind == 'D' then
    local message = { kind = kind, relation = data:u32() }
    message.full  = char(data:u8()) == 'O'
    message.old   = tuple(data)
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

-- Messages that carry an xid inside a stream block
local STREAMED = { R = true, Y = true, I = true, U = true, D = true, T = true, M = true }

-- Decodes one message from the payload of XLogData; streamed tells that a
-- stream block is open
local function decode(payload, streamed)
  local data = protocol.reader(payload)
  local kind = char(data:u8())
  local xid
  if streamed and STREAMED[kind] then
    xid = data:u32()
  end
  if kind == 'S' then
    return { kind = kind, xid = data:u32(), first = data:u8() == 1 }
  elseif kind == 'E' then
    return { kind = kind }
  elseif kind == 'c' then
    local message = { kind = kind, xid = data:u32() }
    data:u8() -- flags
    message.lsn    = data:u64()
    message.finish = data:u64()
    message.time   = time(data)
    return message
  elseif kind == 'A' then
    return { kind = kind, xid = data:u32(), subxid = data:u32() }
  end
  local message = decode_change(kind, data)
  if xid ~= nil then
    message.xid = xid
  end
  return message
end

return {
  EPOCH  = EPOCH,
  lsn    = lsn,
  format = format,
  decode = decode
}
