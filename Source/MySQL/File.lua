-- MySQL dialect without GTIDs (gtid_mode OFF or permissive): the position is
-- a binlog file and an offset in it, file:<server_uuid>:<file>:<offset>,
-- the stream request COM_BINLOG_DUMP. A position belongs to the server that
-- wrote it: on another one (the next of hosts) it means a full load.

local protocol = require('MirrorFlayer.MySQL.Protocol')
local errors   = require('MirrorFlayer.Failure')

local format = string.format

local COM_BINLOG_DUMP = 0x12


-- Parses 'file:uuid:binlog.000003:1234' into { server, file, offset }
local function parse(state)
  if state == nil or state == '' then
    return { server = '', file = '', offset = 4 }
  end
  local uuid, file, offset = string.match(state, '^file:([^:]+):(.+):(%d+)$')
  if uuid == nil then
    errors.reload('the position %s is not a binlog file and offset, a full load follows', state)
  end
  return { server = uuid, file = file, offset = tonumber(offset) }
end

local function format_state(state)
  return format('file:%s:%s:%d', state.server, state.file, state.offset)
end

-- A transaction ends at the end of its last event: its position is the next
-- one of the file
local function add(state, _, header)
  if header ~= nil and header.position ~= nil and header.position > 0 then
    state.offset = header.position
  end
end

-- ROTATE: the next file, and the offset in it
local function rotate(state, rotation)
  state.file   = rotation.file
  state.offset = rotation.position
end

-- The binlog file and offset of the server now
local function position(connection)
  -- The server, the identity of its binlog files, and its binlog in one round
  -- trip; SHOW MASTER STATUS before MySQL 8.2
  local status, results = pcall(connection.results, connection, 'SELECT @@GLOBAL.server_uuid; SHOW BINARY LOG STATUS')
  if not status then
    results = connection:results('SELECT @@GLOBAL.server_uuid; SHOW MASTER STATUS')
  end
  local rows = results[2]
  if not rows[1] then
    protocol.fatal('the source has no binary log (log_bin)')
  end
  return format_state({ server = string.lower(tostring(results[1][1][1])), file = rows[1][1],
    offset = tonumber(rows[1][2]) })
end

-- GTID_LOG_EVENT, ANONYMOUS_GTID_LOG_EVENT: a transaction begins; its GTID,
-- when it has one, is not the position here
local function gtid()
  return { gtid = 'file', standalone = true }
end

-- Asks the server to stream the binlog from the file and offset; a position
-- of another server means a full load. Returns true when events carry CRC32
local function request(connection, options)
  -- The variables of a replica, the server and the checksum of the binlog in one round trip
  local state     = parse(options.state)
  local heartbeat = math.floor(options.heartbeat * 1e9)
  local _, rows   = connection:query(format('SET @master_binlog_checksum = @@GLOBAL.binlog_checksum, ' ..
    '@source_binlog_checksum = @@GLOBAL.binlog_checksum, @master_heartbeat_period = %d, ' ..
    '@source_heartbeat_period = %d; SELECT @@GLOBAL.server_uuid, @@GLOBAL.binlog_checksum', heartbeat, heartbeat))
  local uuid, checksum = string.lower(tostring(rows[1][1])), rows[1][2]
  if state.server ~= uuid then
    errors.reload('the binlog position is one of server %s, this is %s: a full load follows', state.server, uuid)
  end
  connection:command(string.char(COM_BINLOG_DUMP) .. protocol.u32(state.offset) .. protocol.u16(0) ..
    protocol.u32(options.server) .. state.file)
  return string.upper(tostring(checksum)) == 'CRC32'
end

return {
  position = position,
  parse    = parse,
  format   = format_state,
  add      = add,
  rotate   = rotate,
  gtid     = gtid,
  request  = request
}
