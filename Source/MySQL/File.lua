-- MySQL dialect without GTIDs (gtid_mode OFF or permissive): the position is
-- a binlog file and an offset in it, file:<server_uuid>:<file>:<offset>,
-- the stream request COM_BINLOG_DUMP. A position belongs to the server that
-- wrote it: on another one (the next of hosts) it means a full load.

local protocol = require('MirrorFlayer.MySQL.Protocol')
local errors   = require('MirrorFlayer.Failure')

local format = string.format

local COM_BINLOG_DUMP = 0x12

-- The server of the connection, the identity of its binlog files
local function server(connection)
  return string.lower(tostring(connection:value('SELECT @@GLOBAL.server_uuid')))
end

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
  local status, columns, rows = pcall(connection.query, connection, 'SHOW BINARY LOG STATUS')
  if not status then
    columns, rows = connection:query('SHOW MASTER STATUS')
  end
  if rows[1] == nil then
    protocol.fatal('the source has no binary log (log_bin)')
  end
  return format_state({ server = server(connection), file = rows[1][1], offset = tonumber(rows[1][2]) })
end

-- GTID_LOG_EVENT, ANONYMOUS_GTID_LOG_EVENT: a transaction begins; its GTID,
-- when it has one, is not the position here
local function gtid()
  return { gtid = 'file', standalone = true }
end

-- Asks the server to stream the binlog from the file and offset; a position
-- of another server means a full load. Returns true when events carry CRC32
local function request(connection, options)
  local state = parse(options.state)
  local uuid  = server(connection)
  if state.server ~= uuid then
    errors.reload('the binlog position is one of server %s, this is %s: a full load follows', state.server, uuid)
  end
  local checksum  = connection:value('SELECT @@GLOBAL.binlog_checksum')
  local heartbeat = math.floor(options.heartbeat * 1e9)
  connection:query('SET @master_binlog_checksum = @@GLOBAL.binlog_checksum')
  connection:query('SET @source_binlog_checksum = @@GLOBAL.binlog_checksum')
  connection:query(format('SET @master_heartbeat_period = %d', heartbeat))
  connection:query(format('SET @source_heartbeat_period = %d', heartbeat))
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
