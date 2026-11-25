-- MariaDB dialect: GTID format domain-server-sequence, GTID_EVENT and the
-- stream request (@slave_connect_state and COM_BINLOG_DUMP)

local bit = require('bit')

local protocol = require('MirrorFlayer.MySQL.Protocol')

local COM_BINLOG_DUMP = 0x12

local MARIA_SLAVE_CAPABILITY_GTID = 4

local FL_STANDALONE = 1

-- Current binlog position of the server as a GTID state string
local function position(connection)
  local value = connection:value('SELECT @@GLOBAL.gtid_binlog_pos')
  if value == box.NULL or value == nil then
    return ''
  end
  return value
end

-- Parses a GTID state 'd-s-n,d-s-n' into { [domain] = 'd-s-n' }
local function parse(state)
  local result = {}
  for item in string.gmatch(state or '', '[^,%s]+') do
    local domain = string.match(item, '^(%d+)%-%d+%-%d+$')
    if domain == nil then
      protocol.fatal('bad GTID %q', item)
    end
    result[tonumber(domain)] = item
  end
  return result
end

-- Formats { [domain] = 'd-s-n' } as 'd-s-n,d-s-n' ordered by domain
local function format(state)
  local domains = {}
  for domain in pairs(state) do
    table.insert(domains, domain)
  end
  table.sort(domains)
  local items = {}
  for index, domain in ipairs(domains) do
    items[index] = state[domain]
  end
  return table.concat(items, ',')
end

local function digits(value)
  if type(value) == 'number' then
    return string.format('%.0f', value)
  end
  return (string.gsub(tostring(value), 'U?LL$', ''))
end

-- Puts the GTID of a finished transaction into the state
local function add(state, transaction)
  state[transaction.domain] = transaction.gtid
end

-- GTID_EVENT: { domain, gtid, standalone }
local function gtid(header)
  local data     = header.data
  local sequence = data:u64()
  local domain   = data:u32()
  local flags    = data:u8()
  return {
    domain = domain,
    gtid = string.format('%d-%d-%s', domain, header.server, digits(sequence)),
    standalone = bit.band(flags, FL_STANDALONE) ~= 0
  }
end

-- Asks the server to stream the binlog after the GTID state.
-- options: state, server, heartbeat (seconds)
-- Returns true when events carry CRC32.
local function request(connection, options)
  local checksum = connection:value('SELECT @@GLOBAL.binlog_checksum')
  connection:query('SET @master_binlog_checksum = @@GLOBAL.binlog_checksum')
  connection:query('SET @mariadb_slave_capability = ' .. MARIA_SLAVE_CAPABILITY_GTID)
  connection:query('SET @slave_connect_state = ' .. protocol.literal(options.state))
  connection:query('SET @slave_gtid_strict_mode = 0')
  connection:query('SET @slave_gtid_ignore_duplicates = 0')
  connection:query(string.format('SET @master_heartbeat_period = %d', math.floor(options.heartbeat * 1e9)))
  connection:command(string.char(COM_BINLOG_DUMP) .. protocol.u32(4) .. protocol.u16(0) .. protocol.u32(options.server))
  return string.upper(tostring(checksum)) == 'CRC32'
end

return {
  add = add,
  position = position,
  parse = parse,
  format = format,
  gtid = gtid,
  request = request
}
