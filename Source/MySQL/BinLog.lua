-- Binlog events: header, CRC32, FORMAT_DESCRIPTION, ROTATE, QUERY, XID,
-- TABLE_MAP and WRITE/UPDATE/DELETE_ROWS v1/v2, compressed ones of MariaDB,
-- PARTIAL_UPDATE_ROWS of MySQL; row images FULL, MINIMAL, NOBLOB or
-- FULL_NODUP: a column an image lacks is Column.UNCHANGED

local ffi = require('ffi')
local bit = require('bit')

local protocol = require('MirrorFlayer.MySQL.Protocol')
local column   = require('MirrorFlayer.MySQL.Column')
local JSON     = require('MirrorFlayer.MySQL.JSON')
local convert  = require('MirrorFlayer.Column')

local band, bxor, bnot, rshift, tobit = bit.band, bit.bxor, bit.bnot, bit.rshift, bit.tobit

local NULL = box.NULL

local E = {
  QUERY = 2, STOP = 3, ROTATE = 4, FORMAT_DESCRIPTION = 15, XID = 16,
  TABLE_MAP = 19,
  WRITE_ROWS_V1 = 23, UPDATE_ROWS_V1 = 24, DELETE_ROWS_V1 = 25,
  HEARTBEAT = 27, ROWS_QUERY = 29,
  WRITE_ROWS_V2 = 30, UPDATE_ROWS_V2 = 31, DELETE_ROWS_V2 = 32,
  -- MySQL
  GTID_MYSQL = 33, ANONYMOUS_GTID = 34, PREVIOUS_GTIDS = 35, TRANSACTION_CONTEXT = 36,
  VIEW_CHANGE = 37, XA_PREPARE = 38, PARTIAL_UPDATE_ROWS = 39, TRANSACTION_PAYLOAD = 40,
  HEARTBEAT_V2 = 41, GTID_TAGGED = 42,
  -- MariaDB
  ANNOTATE_ROWS = 160, BINLOG_CHECKPOINT = 161, GTID = 162, GTID_LIST = 163,
  START_ENCRYPTION = 164, QUERY_COMPRESSED = 165,
  WRITE_ROWS_COMPRESSED_V1 = 166, UPDATE_ROWS_COMPRESSED_V1 = 167, DELETE_ROWS_COMPRESSED_V1 = 168,
  WRITE_ROWS_COMPRESSED = 169, UPDATE_ROWS_COMPRESSED = 170, DELETE_ROWS_COMPRESSED = 171
}

local ROWS = {
  [E.WRITE_ROWS_V1]  = { 'insert', 1 }, [E.UPDATE_ROWS_V1] = { 'update', 1 }, [E.DELETE_ROWS_V1] = { 'delete', 1 },
  [E.WRITE_ROWS_V2]  = { 'insert', 2 }, [E.UPDATE_ROWS_V2] = { 'update', 2 }, [E.DELETE_ROWS_V2] = { 'delete', 2 },
  -- MariaDB with log_bin_compress: the rows after the bitmaps are compressed
  [E.WRITE_ROWS_COMPRESSED_V1] = { 'insert', 1, true }, [E.UPDATE_ROWS_COMPRESSED_V1] = { 'update', 1, true },
  [E.DELETE_ROWS_COMPRESSED_V1] = { 'delete', 1, true },
  [E.WRITE_ROWS_COMPRESSED] = { 'insert', 2, true }, [E.UPDATE_ROWS_COMPRESSED] = { 'update', 2, true },
  [E.DELETE_ROWS_COMPRESSED] = { 'delete', 2, true },
  -- MySQL with binlog_row_value_options=PARTIAL_JSON: JSON columns may come
  -- as changes of the old value
  [E.PARTIAL_UPDATE_ROWS] = { 'update', 2, false, true }
}

local UNCHANGED = convert.UNCHANGED
local TYPE_JSON = 245

local HEADER = 19

-- MirrorFlayer.Support: CRC32 and inflate of zlib; without it CRC32 in Lua and no compressed events
local loaded, support = pcall(require, 'MirrorFlayer.Support')

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
  if loaded then
    return tobit(support.crc32(data, length))
  end
  local pointer = ffi.cast('const uint8_t *', data)
  local value   = tobit(0xffffffff)
  for index = 0, length - 1 do
    value = bxor(CRC[band(bxor(value, pointer[index]), 0xff)], rshift(value, 8))
  end
  return bnot(value)
end

-- Compressed content of MariaDB: 0x80 | algorithm << 4 | the count of length bytes, the length before compression
-- (big-endian), then what compress() of zlib made, the only algorithm. Returns a reader of the content
local function inflate(data)
  local first = data:u8()
  if band(first, 0xf0) ~= 0x80 then
    protocol.fatal('compressed binlog event: unknown format 0x%02x', first)
  end
  local length = data:be(band(first, 0x07))
  if not loaded then
    protocol.fatal('compressed binlog events need MirrorFlayer.Support (Support.so): %s', tostring(support))
  end
  return protocol.reader(support.uncompress(data:rest(), length))
end

-- TRANSACTION_PAYLOAD of MySQL (binlog_transaction_compression): fields of
-- a type, a length and a value, all length-encoded integers, up to the end
-- mark 0: 1 the size of the payload, 2 the compression (0 zstd, 255 none),
-- 3 the size before compression. The payload is the events of the
-- transaction one after another. Returns them as packets for event()
local function payload(header)
  local data = header.data
  local size, compression, original
  while true do
    local field = data:lenenc()
    if field == 0 then
      break
    end
    local length = data:lenenc()
    if field == 1 then
      size = data:lenenc()
    elseif field == 2 then
      compression = data:lenenc()
    elseif field == 3 then
      original = data:lenenc()
    else
      data:skip(length)
    end
  end
  local content = data:bytes(size or data:left())
  if compression == 0 then
    if not loaded then
      protocol.fatal('compressed transactions need MirrorFlayer.Support (Support.so): %s', tostring(support))
    end
    if original == nil then
      protocol.fatal('compressed transaction without its size')
    end
    content = support.uncompress(content, original, 'zstd')
  elseif compression ~= 255 then
    protocol.fatal('compressed transaction: unknown compression %s', tostring(compression))
  end
  local events, position = {}, 1
  while position <= #content do
    local length = protocol.reader(content, position + 9):u32()
    table.insert(events, string.sub(content, position, position + length - 1))
    position = position + length
  end
  return events
end

-- Events ---------------------------------------------------------------------

-- Parses an event of the dump stream. packet is the payload after the 0x00
-- marker byte; checksum tells that events end with CRC32.
-- Returns the header with data (a reader limited to the body).
local function event(packet, checksum)
  local data   = protocol.reader(packet)
  local header = {
    timestamp = data:u32(),
    type      = data:u8(),
    server    = data:u32(),
    size      = data:u32(),
    position  = data:u32(),
    flags     = data:u16()
  }
  local limit = #packet

  if header.type == E.FORMAT_DESCRIPTION then
    -- Always carries the algorithm byte and four checksum bytes
    header.checksum = string.byte(packet, limit - 4)
    checksum        = header.checksum == 1
  end

  if checksum and header.type ~= E.HEARTBEAT and header.type ~= E.HEARTBEAT_V2 then
    local stored  = data.limit - 3
    data.position = stored
    stored        = tobit(data:u32())
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
  data.limit  = limit
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
  if header.type == E.QUERY_COMPRESSED then data = inflate(data) end
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
  local count  = data:lenenc()
  result.types = {}
  for index = 1, count do
    result.types[index] = data:u8()
  end
  local length = data:lenenc()
  local stop   = data.position + length
  result.meta  = {}
  for index = 1, count do
    result.meta[index] = column.meta(data, result.types[index])
  end
  data.position = stop
  return result
end

-- Table id of a rows event, to look up the table map before decoding
local function rows_table(header)
  local data    = header.data
  local saved   = data.position
  local id      = data:u48()
  data.position = saved
  return id
end

-- A partial JSON value: the diffs applied to the old value when the before
-- image has it, a patch of the stored value otherwise
local function partial(data, description, old)
  local diffs = JSON.diffs(data:bytes(data:u32()))
  -- box.NULL == nil as well
  if old ~= nil and not rawequal(old, UNCHANGED) then
    return JSON.patch(old, diffs, description.json)
  end
  return convert.patch('MirrorFlayer.MySQL.JSON', diffs, description.json)
end

-- A row image: present tells the columns it has, the others are UNCHANGED;
-- partials are the JSON columns sent as diffs, base the before image
local function image(data, map, columns, present, partials, base)
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
      elseif partials ~= nil and partials[index] then
        row[index] = partial(data, columns[index], base[index])
      else
        row[index] = column.decode(data, columns[index], map.types[index], map.meta[index])
      end
    else
      row[index] = UNCHANGED
    end
  end
  return row
end

-- The shared image of PARTIAL_UPDATE_ROWS: value options and, with partial
-- JSON updates, a bit for every JSON column of the table; the JSON columns
-- sent as diffs, by index
local function shared(data, map)
  if band(data:lenenc(), 1) == 0 then
    return nil
  end
  local indexes = {}
  for index, kind in ipairs(map.types) do
    if kind == TYPE_JSON then
      table.insert(indexes, index)
    end
  end
  local bits, result = bitmap(data, #indexes), {}
  for position, index in ipairs(indexes) do
    if bits[position] then
      result[index] = true
    end
  end
  return result
end

-- WRITE/UPDATE/DELETE_ROWS v1/v2.
-- map is the TABLE_MAP of the table, columns its column descriptions.
-- Returns the operation and an array of { old, new } pairs.
local function rows(header, map, columns)
  local kind = ROWS[header.type]
  local data = header.data
  local operation, version, compressed, partials = kind[1], kind[2], kind[3], kind[4]
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
  if compressed then data = inflate(data) end
  local result = {}
  while data:left() > 0 do
    local first = image(data, map, columns, before)
    if operation == 'insert' then
      table.insert(result, { nil, first })
    elseif operation == 'delete' then
      table.insert(result, { first, nil })
    else
      table.insert(result, { first, image(data, map, columns, after, partials and shared(data, map), first) })
    end
  end
  return operation, result
end

return {
  E          = E,
  ROWS       = ROWS,
  crc32      = crc32,
  event      = event,
  rotate     = rotate,
  query      = query,
  xid        = xid,
  table_map  = table_map,
  payload    = payload,
  rows_table = rows_table,
  rows       = rows
}
