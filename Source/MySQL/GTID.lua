-- MySQL dialect: GTID sets uuid:1-100:105, with tags of MySQL 8.3 and later
-- uuid:1-5:tag:1-3, GTID_LOG_EVENT, GTID_TAGGED_LOG_EVENT and the stream
-- request COM_BINLOG_DUMP_GTID. The source needs gtid_mode=ON.
--
-- A set is { [tsid] = intervals }, tsid the uuid or 'uuid:tag'.

local ffi = require('ffi')
local bit = require('bit')

local protocol = require('MirrorFlayer.MySQL.Protocol')

local char, format, gsub, sub, byte = string.char, string.format, string.gsub, string.sub, string.byte
local band = bit.band

local GTID_TAGGED = 42

local COM_BINLOG_DUMP_GTID = 0x1e
local BINLOG_THROUGH_GTID  = 0x04

local UUID = '^%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x$'

local function digits(value)
  if type(value) == 'number' then
    return format('%.0f', value)
  end
  return (gsub(tostring(value), 'U?LL$', ''))
end

-- Sorts and merges intervals { first, last } in place
local function normalize(intervals)
  table.sort(intervals, function (left, right) return left[1] < right[1] end)
  local merged = {}
  for _, interval in ipairs(intervals) do
    local last = merged[#merged]
    if last ~= nil and interval[1] <= last[2] + 1 then
      last[2] = math.max(last[2], interval[2])
    else
      table.insert(merged, { interval[1], interval[2] })
    end
  end
  return merged
end

-- Parses a GTID set 'uuid:1-5:7:tag:1-3,uuid:1-3' into
-- { [uuid] = { { 1, 5 }, { 7, 7 } }, ['uuid:tag'] = { { 1, 3 } }, ... };
-- tags are lowercase, as MySQL keeps them
local function parse(state)
  local result = {}
  for item in string.gmatch(gsub(state or '', '%s', ''), '[^,]+') do
    local parts = {}
    for part in string.gmatch(item, '[^:]+') do
      table.insert(parts, part)
    end
    local uuid = string.lower(parts[1] or '')
    if string.match(uuid, UUID) == nil then
      protocol.fatal('bad GTID set item %q', item)
    end
    local tsid = uuid
    for index = 2, #parts do
      local first, last = string.match(parts[index], '^(%d+)%-(%d+)$')
      if first == nil then
        first = string.match(parts[index], '^(%d+)$')
        last  = first
      end
      if first == nil then
        local tag = string.lower(parts[index])
        if string.match(tag, '^[%a_][%w_]*$') == nil or #tag > 32 then
          protocol.fatal('bad GTID set item %q', item)
        end
        tsid = uuid .. ':' .. tag
      else
        result[tsid] = result[tsid] or {}
        table.insert(result[tsid], { tonumber(first), tonumber(last) })
      end
    end
  end
  for tsid, intervals in pairs(result) do
    result[tsid] = normalize(intervals)
  end
  return result
end

local function intervals_text(parts, intervals)
  for _, interval in ipairs(intervals) do
    if interval[1] == interval[2] then
      table.insert(parts, digits(interval[1]))
    else
      table.insert(parts, digits(interval[1]) .. '-' .. digits(interval[2]))
    end
  end
end

-- Formats a set as MySQL prints it: ordered by uuid, the untagged intervals
-- of a uuid first, then its tags in order
local function format_set(state)
  local tsids = {}
  for tsid in pairs(state) do
    table.insert(tsids, tsid)
  end
  table.sort(tsids)
  local items, current, parts = {}, nil, nil
  for _, tsid in ipairs(tsids) do
    local uuid, tag = sub(tsid, 1, 36), sub(tsid, 38)
    if uuid ~= current then
      if parts ~= nil and #parts > 1 then
        table.insert(items, table.concat(parts, ':'))
      end
      current, parts = uuid, { uuid }
    end
    if #state[tsid] > 0 then
      if tag ~= '' then
        table.insert(parts, tag)
      end
      intervals_text(parts, state[tsid])
    end
  end
  if parts ~= nil and #parts > 1 then
    table.insert(items, table.concat(parts, ':'))
  end
  return table.concat(items, ',')
end

-- Adds the GTID of a finished transaction to the set
local function add(state, transaction)
  local intervals = state[transaction.tsid]
  local gno       = transaction.gno
  if intervals == nil then
    state[transaction.tsid] = { { gno, gno } }
    return
  end
  local last = intervals[#intervals]
  if last ~= nil and gno == last[2] + 1 then
    last[2] = gno
    return
  end
  table.insert(intervals, { gno, gno })
  state[transaction.tsid] = normalize(intervals)
end

-- Current binlog position of the server as a GTID set string
local function position(connection)
  local mode = tostring(connection:value('SELECT @@GLOBAL.gtid_mode'))
  if string.upper(mode) ~= 'ON' then
    protocol.fatal('gtid_mode of the source is %s, MirrorFlayer needs ON', mode)
  end
  local value = connection:value('SELECT @@GLOBAL.gtid_executed')
  if value == box.NULL or value == nil then
    return ''
  end
  return format_set(parse(value))
end

-- Variable-length integer of the serialization of MySQL (8.3 and later): the
-- count of trailing ones of the first byte plus one is the count of bytes,
-- little-endian, the value above those bits; 9 bytes for a first byte 0xff
local function varlen(data)
  local first = data:u8()
  local count = 1
  while count < 9 and band(first, bit.lshift(1, count - 1)) ~= 0 do
    count = count + 1
  end
  if count == 9 then
    return data:u64()
  end
  local raw   = char(first) .. data:bytes(count - 1) .. string.rep('\0', 8 - count)
  local value = ffi.cast('const uint64_t *', raw)[0] / bit.lshift(1ULL, count)
  return value < 0x20000000000000ULL and tonumber(value) or value
end

-- Signed: the lowest bit is the sign, (-x - 1) for negatives
local function varlen_signed(data)
  local value = varlen(data)
  if value % 2 == 1 then
    return -((value - 1) / 2) - 1
  end
  return value / 2
end

-- A string: its length and bytes
local function varlen_string(data)
  return data:bytes(varlen(data))
end

local function uuid_text(sid)
  local hex = gsub(sid, '.', function (octet) return format('%02x', byte(octet)) end)
  return format('%s-%s-%s-%s-%s', hex:sub(1, 8), hex:sub(9, 12), hex:sub(13, 16), hex:sub(17, 20), hex:sub(21, 32))
end

-- GTID_TAGGED_LOG_EVENT: a version byte, the size of the message and the id
-- of its last field that cannot be ignored, then fields by their ids: 0 the
-- flags, 1 the uuid (16 bytes), 2 the gno (signed), 3 the tag, others not
-- needed here
local function tagged(data)
  local uuid, gno, tag
  data:skip(1) -- version
  local size = varlen(data)
  local stop = data.position + size - 1
  varlen(data) -- the last field that cannot be ignored
  while data.position <= math.min(stop, data.limit) and (uuid == nil or gno == nil or tag == nil) do
    local id = varlen(data)
    if id == 0 then
      varlen(data)
    elseif id == 1 then
      local sid = {}
      for index = 1, 16 do
        sid[index] = char(varlen(data))
      end
      uuid = uuid_text(table.concat(sid))
    elseif id == 2 then
      gno = varlen_signed(data)
    elseif id == 3 then
      tag = string.lower(varlen_string(data))
    else
      break
    end
  end
  if uuid == nil or gno == nil then
    protocol.fatal('GTID_TAGGED_LOG_EVENT without its uuid or gno')
  end
  return uuid, gno, tag or ''
end

-- GTID_LOG_EVENT or GTID_TAGGED_LOG_EVENT: { tsid, gno, gtid, standalone };
-- standalone until BEGIN comes, a DDL statement has none
local function gtid(header)
  local data = header.data
  local uuid, gno, tag
  if header.type == GTID_TAGGED then
    uuid, gno, tag = tagged(data)
  else
    data:skip(1) -- flags
    uuid = uuid_text(data:bytes(16))
    gno  = data:i64()
    tag  = ''
  end
  local tsid = tag ~= '' and uuid .. ':' .. tag or uuid
  return {
    tsid       = tsid,
    gno        = gno,
    gtid       = tsid .. ':' .. digits(gno),
    standalone = true
  }
end

-- Binary GTID set of COM_BINLOG_DUMP_GTID. With a tag the tagged format of
-- MySQL 8.3: the count of tsids as (1 << 56 | count << 8 | 1), a tag after
-- each uuid as a length and the bytes (an empty one for none)
local function encode(state)
  local tsids, tagged = {}, false
  for tsid in pairs(state) do
    table.insert(tsids, tsid)
    tagged = tagged or #tsid > 36
  end
  table.sort(tsids)
  local parts = { protocol.u64(#tsids) }
  if tagged then
    parts[1] = '\1' .. sub(protocol.u64(#tsids), 1, 6) .. '\1'
  end
  for _, tsid in ipairs(tsids) do
    local intervals = state[tsid]
    local uuid, tag = sub(tsid, 1, 36), sub(tsid, 38)
    table.insert(parts, (gsub(gsub(uuid, '%-', ''), '%x%x', function (pair) return char(tonumber(pair, 16)) end)))
    if tagged then
      -- Tags are at most 32 bytes: one byte of length, shifted past its marker bit
      table.insert(parts, char(#tag * 2) .. tag)
    end
    table.insert(parts, protocol.u64(#intervals))
    for _, interval in ipairs(intervals) do
      table.insert(parts, protocol.u64(interval[1]) .. protocol.u64(interval[2] + 1))
    end
  end
  return table.concat(parts)
end

-- Asks the server to stream the binlog after the GTID set.
-- options: state, server, heartbeat (seconds)
-- Returns true when events carry CRC32.
local function request(connection, options)
  local mode = tostring(connection:value('SELECT @@GLOBAL.gtid_mode'))
  if string.upper(mode) ~= 'ON' then
    protocol.fatal('gtid_mode of the source is %s, MirrorFlayer needs ON', mode)
  end
  local checksum  = connection:value('SELECT @@GLOBAL.binlog_checksum')
  local heartbeat = math.floor(options.heartbeat * 1e9)
  connection:query('SET @master_binlog_checksum = @@GLOBAL.binlog_checksum')
  connection:query('SET @source_binlog_checksum = @@GLOBAL.binlog_checksum')
  connection:query(format('SET @master_heartbeat_period = %d', heartbeat))
  connection:query(format('SET @source_heartbeat_period = %d', heartbeat))
  local data = encode(parse(options.state))
  connection:command(char(COM_BINLOG_DUMP_GTID) .. protocol.u16(BINLOG_THROUGH_GTID) .. protocol.u32(options.server) ..
    protocol.u32(0) .. protocol.u64(4) .. protocol.u32(#data) .. data)
  return string.upper(tostring(checksum)) == 'CRC32'
end

return {
  position = position,
  parse    = parse,
  format   = format_set,
  add      = add,
  gtid     = gtid,
  request  = request
}
