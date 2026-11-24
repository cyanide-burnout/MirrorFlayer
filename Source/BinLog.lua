-- Binlog events: header, CRC32, FORMAT_DESCRIPTION, ROTATE, QUERY, XID,
-- TABLE_MAP and WRITE/UPDATE/DELETE_ROWS v1/v2

local ffi = require('ffi')
local bit = require('bit')

local protocol = require('MirrorFlayer.Protocol')
local column   = require('MirrorFlayer.Column')

local band, bxor, bnot, rshift, tobit = bit.band, bit.bxor, bit.bnot, bit.rshift, bit.tobit

local NULL = box.NULL

local E = {
  QUERY = 2, STOP = 3, ROTATE = 4, FORMAT_DESCRIPTION = 15, XID = 16,
  TABLE_MAP = 19,
  WRITE_ROWS_V1 = 23, UPDATE_ROWS_V1 = 24, DELETE_ROWS_V1 = 25,
  HEARTBEAT = 27,
  WRITE_ROWS_V2 = 30, UPDATE_ROWS_V2 = 31, DELETE_ROWS_V2 = 32,
  -- MariaDB
  ANNOTATE_ROWS = 160, BINLOG_CHECKPOINT = 161, GTID = 162, GTID_LIST = 163,
  START_ENCRYPTION = 164, QUERY_COMPRESSED = 165,
  WRITE_ROWS_COMPRESSED_V1 = 166, UPDATE_ROWS_COMPRESSED_V1 = 167, DELETE_ROWS_COMPRESSED_V1 = 168,
  WRITE_ROWS_COMPRESSED = 169, UPDATE_ROWS_COMPRESSED = 170, DELETE_ROWS_COMPRESSED = 171
}

local ROWS = {
  [E.WRITE_ROWS_V1]  = { 'insert', 1 }, [E.UPDATE_ROWS_V1] = { 'update', 1 }, [E.DELETE_ROWS_V1] = { 'delete', 1 },
  [E.WRITE_ROWS_V2]  = { 'insert', 2 }, [E.UPDATE_ROWS_V2] = { 'update', 2 }, [E.DELETE_ROWS_V2] = { 'delete', 2 }
}

local HEADER = 19

-- CRC32 (IEEE 802.3, as zlib) ------------------------------------------------

local CRC = ffi.new('int32_t[256]')
for index = 0, 255 do
  local value = index
  for _ = 1, 8 do
    if band(value, 1) ~= 0 then
      value = bxor(rshift(value, 1), tobit(0xEDB88320))
    else
      value = rshift(value, 1)
    end
  end
  CRC[index] = value
end

local function crc32(data, length)
  local pointer = ffi.cast('const uint8_t *', data)
  local value   = tobit(0xffffffff)
  for index = 0, length - 1 do
    value = bxor(CRC[band(bxor(value, pointer[index]), 0xff)], rshift(value, 8))
  end
  return bnot(value)
end

-- Events ---------------------------------------------------------------------

-- Parses an event of the dump stream. packet is the payload after the 0x00
-- marker byte; checksum tells that events end with CRC32.
-- Returns the header with data (a reader limited to the body).
local function event(packet, checksum)
  local data   = protocol.reader(packet)
  local header = {
    timestamp = data:u32(),
    type = data:u8(),
    server = data:u32(),
    size = data:u32(),
    position = data:u32(),
    flags = data:u16()
  }
  local limit = #packet

  if header.type == E.FORMAT_DESCRIPTION then
    -- Always carries the algorithm byte and four checksum bytes
    header.checksum = string.byte(packet, limit - 4)
    checksum = header.checksum == 1
  end

  if checksum and header.type ~= E.HEARTBEAT then
    local stored = data.limit - 3
    data.position = stored
    stored = tobit(data:u32())
    if stored ~= crc32(packet, limit - 4) then
      error(protocol.failure(string.format('binlog event %d at %d: checksum mismatch', header.type, header.position)), 0)
    end
    data.position = HEADER + 1
  end
  if checksum or header.type == E.FORMAT_DESCRIPTION then
    limit = limit - 4
  end
  if header.type == E.FORMAT_DESCRIPTION then
    limit = limit - 1
  end
  data.limit = limit
  header.data = data
  return header
end

local function rotate(header)
  local data     = header.data
  local position = data:u64()
  return { position = position, file = data:rest() }
end

local function query(header)
  local data = header.data
  data:skip(4 + 4) -- thread, execution time
  local length = data:u8()
  data:skip(2) -- error code
  local status = data:u16()
  data:skip(status)
  local schema = data:bytes(length)
  data:skip(1)
  return { schema = schema, query = data:rest() }
end

local function xid(header)
  return header.data:u64()
end

local function bitmap(data, count)
  local raw    = data:bytes(math.floor((count + 7) / 8))
  local result = {}
  for index = 0, count - 1 do
    result[index + 1] = band(string.byte(raw, math.floor(index / 8) + 1), bit.lshift(1, index % 8)) ~= 0
  end
  return result
end

-- TABLE_MAP_EVENT: { id, schema, table, types, meta }
local function table_map(header)
  local data   = header.data
  local result = { id = data:u48() }
  data:skip(2) -- flags
  result.schema = data:bytes(data:u8())
  data:skip(1)
  result.table = data:bytes(data:u8())
  data:skip(1)
  local count = data:lenenc()
  result.types = {}
  for index = 1, count do
    result.types[index] = data:u8()
  end
  local length = data:lenenc()
  local stop   = data.position + length
  result.meta = {}
  for index = 1, count do
    result.meta[index] = column.meta(data, result.types[index])
  end
  data.position = stop
  return result
end

-- Table id of a rows event, to look up the table map before decoding
local function rows_table(header)
  local data  = header.data
  local saved = data.position
  local id    = data:u48()
  data.position = saved
  return id
end

local function full(present, what)
  for index = 1, #present do
    if not present[index] then
      protocol.fatal('%s row image lacks column %d: binlog_row_image must be FULL', what, index)
    end
  end
end

local function image(data, map, columns, present)
  local count = 0
  for index = 1, #present do
    if present[index] then
      count = count + 1
    end
  end
  local nulls = bitmap(data, count)
  local row   = {}
  local slot  = 0
  for index = 1, #present do
    if present[index] then
      slot = slot + 1
      if nulls[slot] then
        row[index] = NULL
      else
        row[index] = column.decode(data, columns[index], map.types[index], map.meta[index])
      end
    else
      row[index] = NULL
    end
  end
  return row
end

-- WRITE/UPDATE/DELETE_ROWS v1/v2.
-- map is the TABLE_MAP of the table, columns its column descriptions.
-- Returns the operation and an array of { old, new } pairs.
local function rows(header, map, columns)
  local kind = ROWS[header.type]
  local data = header.data
  local operation, version = kind[1], kind[2]
  data:skip(6 + 2) -- table id, flags
  if version == 2 then
    data:skip(data:u16() - 2)
  end
  local count = data:lenenc()
  if count ~= #map.types then
    protocol.fatal('%s.%s: rows event has %d columns, table map %d', map.schema, map.table, count, #map.types)
  end
  local before = bitmap(data, count)
  local after  = before
  if operation == 'update' then
    after = bitmap(data, count)
  end
  full(before, operation == 'insert' and 'after' or 'before')
  if operation == 'update' then
    full(after, 'after')
  end
  local result = {}
  while data:left() > 0 do
    local first = image(data, map, columns, before)
    if operation == 'insert' then
      table.insert(result, { nil, first })
    elseif operation == 'delete' then
      table.insert(result, { first, nil })
    else
      table.insert(result, { first, image(data, map, columns, after) })
    end
  end
  return operation, result
end

return {
  E = E,
  ROWS = ROWS,
  crc32 = crc32,
  event = event,
  rotate = rotate,
  query = query,
  xid = xid,
  table_map = table_map,
  rows_table = rows_table,
  rows = rows
}
