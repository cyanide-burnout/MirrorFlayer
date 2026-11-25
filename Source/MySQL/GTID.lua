-- MySQL dialect: GTID sets uuid:1-100:105, GTID_LOG_EVENT and the stream
-- request COM_BINLOG_DUMP_GTID. The source needs gtid_mode=ON.

local protocol = require('MirrorFlayer.MySQL.Protocol')

local char, format, gsub = string.char, string.format, string.gsub

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

-- Parses a GTID set 'uuid:1-5:7,uuid:1-3' into { [uuid] = { { 1, 5 }, { 7, 7 } } }
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
    local intervals = result[uuid] or {}
    for index = 2, #parts do
      local first, last = string.match(parts[index], '^(%d+)%-(%d+)$')
      if first == nil then
        first = string.match(parts[index], '^(%d+)$')
        last  = first
      end
      if first == nil then
        protocol.fatal('tagged GTIDs (%s) are not supported', item)
      end
      table.insert(intervals, { tonumber(first), tonumber(last) })
    end
    result[uuid] = normalize(intervals)
  end
  return result
end

-- Formats { [uuid] = intervals } as MySQL prints it, ordered by uuid
local function format_set(state)
  local uuids = {}
  for uuid in pairs(state) do
    table.insert(uuids, uuid)
  end
  table.sort(uuids)
  local items = {}
  for _, uuid in ipairs(uuids) do
    local parts = { uuid }
    for _, interval in ipairs(state[uuid]) do
      if interval[1] == interval[2] then
        table.insert(parts, digits(interval[1]))
      else
        table.insert(parts, digits(interval[1]) .. '-' .. digits(interval[2]))
      end
    end
    if #parts > 1 then
      table.insert(items, table.concat(parts, ':'))
    end
  end
  return table.concat(items, ',')
end

-- Adds the GTID of a finished transaction to the set
local function add(state, transaction)
  local intervals = state[transaction.uuid]
  local gno       = transaction.gno
  if intervals == nil then
    state[transaction.uuid] = { { gno, gno } }
    return
  end
  local last = intervals[#intervals]
  if last ~= nil and gno == last[2] + 1 then
    last[2] = gno
    return
  end
  table.insert(intervals, { gno, gno })
  state[transaction.uuid] = normalize(intervals)
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

-- GTID_LOG_EVENT: { uuid, gno, gtid, standalone }; standalone until BEGIN
-- comes, a DDL statement has none
local function gtid(header)
  local data = header.data
  data:skip(1) -- flags
  local sid  = data:bytes(16)
  local gno  = data:i64()
  local hex  = gsub(sid, '.', function (octet) return format('%02x', string.byte(octet)) end)
  local uuid = format('%s-%s-%s-%s-%s', hex:sub(1, 8), hex:sub(9, 12), hex:sub(13, 16), hex:sub(17, 20), hex:sub(21, 32))
  return {
    uuid       = uuid,
    gno        = gno,
    gtid       = uuid .. ':' .. digits(gno),
    standalone = true
  }
end

-- Binary GTID set of COM_BINLOG_DUMP_GTID
local function encode(state)
  local uuids = {}
  for uuid in pairs(state) do
    table.insert(uuids, uuid)
  end
  table.sort(uuids)
  local parts = { protocol.u64(#uuids) }
  for _, uuid in ipairs(uuids) do
    local intervals = state[uuid]
    table.insert(parts, (gsub(gsub(uuid, '%-', ''), '%x%x', function (pair) return char(tonumber(pair, 16)) end)))
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
