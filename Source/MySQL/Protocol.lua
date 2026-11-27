-- MySQL client/server protocol: packets, greeting, authentication and
-- COM_QUERY with a streamed text result set

local ffi    = require('ffi')
local bit    = require('bit')
local digest = require('digest')

local transport = require('MirrorFlayer.Transport')
local errors    = require('MirrorFlayer.Failure')

local band, bor, bxor = bit.band, bit.bor, bit.bxor
local byte, char, sub = string.byte, string.char, string.sub

local NULL = box.NULL

local CLIENT_LONG_PASSWORD                  = 0x00000001
local CLIENT_LONG_FLAG                      = 0x00000004
local CLIENT_CONNECT_WITH_DB                = 0x00000008
local CLIENT_COMPRESS                       = 0x00000020
local CLIENT_PROTOCOL_41                    = 0x00000200
local CLIENT_SSL                            = 0x00000800
local CLIENT_TRANSACTIONS                   = 0x00002000
local CLIENT_SECURE_CONNECTION              = 0x00008000
local CLIENT_MULTI_RESULTS                  = 0x00020000
local CLIENT_PLUGIN_AUTH                    = 0x00080000
local CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000

local CHARSET_UTF8MB4_GENERAL_CI = 45

local COM_QUIT  = 0x01
local COM_QUERY = 0x03

local failure, fatal = errors.failure, errors.fatal

-- Reader over a string -------------------------------------------------------

local Reader   = {}
Reader.__index = Reader

local function reader(data, position, limit)
  return setmetatable({
    data     = data,
    pointer  = ffi.cast('const uint8_t *', data),
    position = position or 1,
    limit    = limit or #data
  }, Reader)
end

function Reader:left()
  return self.limit - self.position + 1
end

function Reader:check(n)
  if self.position + n - 1 > self.limit then
    error(failure(string.format('truncated packet: need %d bytes at %d of %d', n, self.position, self.limit)), 0)
  end
end

function Reader:skip(n)
  self:check(n)
  self.position = self.position + n
end

function Reader:u8()
  self:check(1)
  local value   = byte(self.data, self.position)
  self.position = self.position + 1
  return value
end

function Reader:u16()
  self:check(2)
  local a, b = byte(self.data, self.position, self.position + 1)
  self.position = self.position + 2
  return a + b * 0x100
end

function Reader:u24()
  self:check(3)
  local a, b, c = byte(self.data, self.position, self.position + 2)
  self.position = self.position + 3
  return a + b * 0x100 + c * 0x10000
end

function Reader:u32()
  self:check(4)
  local a, b, c, d = byte(self.data, self.position, self.position + 3)
  self.position = self.position + 4
  return a + b * 0x100 + c * 0x10000 + d * 0x1000000
end

function Reader:u48()
  local low = self:u32()
  return low + self:u16() * 0x100000000
end

-- Integers above 2^53 come as uint64_t / int64_t cdata, the rest as numbers.
-- The pointers are not const: msgpack refuses const-qualified cdata.
function Reader:u64()
  local value
  self:check(8)
  value         = ffi.cast('uint64_t *', self.pointer + self.position - 1)[0]
  self.position = self.position + 8
  if value < 0x20000000000000ULL then
    return tonumber(value)
  end
  return value
end

function Reader:i64()
  local value
  self:check(8)
  value         = ffi.cast('int64_t *', self.pointer + self.position - 1)[0]
  self.position = self.position + 8
  if value < 0x20000000000000LL and value > -0x20000000000000LL then
    return tonumber(value)
  end
  return value
end

-- Little-endian signed integer of n bytes (n <= 4)
function Reader:int(n)
  local value, limit
  if n == 1 then value = self:u8()
  elseif n == 2 then value = self:u16()
  elseif n == 3 then value = self:u24()
  else value = self:u32() end
  limit = 2 ^ (8 * n)
  if value >= limit / 2 then
    value = value - limit
  end
  return value
end

-- Big-endian unsigned integer of n bytes (n <= 6)
function Reader:be(n)
  local value = 0
  self:check(n)
  for index = self.position, self.position + n - 1 do
    value = value * 0x100 + byte(self.data, index)
  end
  self.position = self.position + n
  return value
end

function Reader:float()
  local value
  self:check(4)
  value         = ffi.cast('const float *', self.pointer + self.position - 1)[0]
  self.position = self.position + 4
  return tonumber(value)
end

function Reader:double()
  local value
  self:check(8)
  value         = ffi.cast('const double *', self.pointer + self.position - 1)[0]
  self.position = self.position + 8
  return tonumber(value)
end

function Reader:bytes(n)
  self:check(n)
  local value   = sub(self.data, self.position, self.position + n - 1)
  self.position = self.position + n
  return value
end

-- NUL-terminated string; up to the end when the terminator is missing
function Reader:cstring()
  local stop = string.find(self.data, '\0', self.position, true)
  local value
  if stop == nil or stop > self.limit then
    value         = sub(self.data, self.position, self.limit)
    self.position = self.limit + 1
    return value
  end
  value         = sub(self.data, self.position, stop - 1)
  self.position = stop + 1
  return value
end

function Reader:rest()
  local value   = sub(self.data, self.position, self.limit)
  self.position = self.limit + 1
  return value
end

-- Length-encoded integer; nil for the NULL marker 0xFB
function Reader:lenenc()
  local first = self:u8()
  if first < 0xfb then
    return first
  end
  if first == 0xfb then
    return nil
  end
  if first == 0xfc then
    return self:u16()
  end
  if first == 0xfd then
    return self:u24()
  end
  return self:u64()
end

-- Length-encoded string; NULL for the NULL marker
function Reader:lenstr()
  local length = self:lenenc()
  if length == nil then
    return NULL
  end
  return self:bytes(length)
end

-- Encoding -------------------------------------------------------------------

local function u16(value)
  return char(value % 0x100, math.floor(value / 0x100) % 0x100)
end

local function u32(value)
  return char(value % 0x100, math.floor(value / 0x100) % 0x100,
    math.floor(value / 0x10000) % 0x100, math.floor(value / 0x1000000) % 0x100)
end

local function u64(value)
  return u32(value % 0x100000000) .. u32(math.floor(value / 0x100000000))
end

local function lenenc(value)
  if value < 0xfb then
    return char(value)
  end
  if value < 0x10000 then
    return '\252' .. u16(value)
  end
  if value < 0x1000000 then
    return '\253' .. sub(u32(value), 1, 3)
  end
  return '\254' .. u32(value % 0x100000000) .. u32(math.floor(value / 0x100000000))
end

-- Compressed protocol ---------------------------------------------------------

-- After the authentication packets go in frames: 3 bytes of the length, the sequence of the frame, 3 bytes of the
-- length before compression (0 when the frame is not compressed); a frame holds packets or a part of one. The
-- client sends its frames uncompressed, the server compresses what is worth it.
local Compressed   = {}
Compressed.__index = Compressed

function Compressed:read(n, timeout)
  while #self.buffer - self.offset + 1 < n do
    local header   = self.stream:read(7, timeout)
    local length   = byte(header, 1) + byte(header, 2) * 0x100 + byte(header, 3) * 0x10000
    local original = byte(header, 5) + byte(header, 6) * 0x100 + byte(header, 7) * 0x10000
    local payload  = length > 0 and self.stream:read(length, timeout) or ''
    if original > 0 then payload = self.uncompress(payload, original) end
    self.sequence            = (byte(header, 4) + 1) % 0x100
    self.buffer, self.offset = sub(self.buffer, self.offset) .. payload, 1
  end
  local data  = sub(self.buffer, self.offset, self.offset + n - 1)
  self.offset = self.offset + n
  if self.offset > #self.buffer then self.buffer, self.offset = '', 1 end
  return data
end

function Compressed:write(data, timeout)
  local offset = 1
  repeat
    local part = sub(data, offset, offset + 0xfffffe)
    self.stream:write(sub(u32(#part), 1, 3) .. char(self.sequence) .. '\0\0\0' .. part, timeout)
    self.sequence = (self.sequence + 1) % 0x100
    offset        = offset + #part
  until offset > #data
end

function Compressed:wait(timeout)
  return #self.buffer >= self.offset or self.stream:wait(timeout)
end

function Compressed:close()
  self.stream:close()
end

function Compressed:fd()
  return self.stream:fd()
end

-- Quotes a string literal for SQL
local function literal(value)
  return "'" .. string.gsub(value, "[%z\n\r\\'\"\026]", {
    ['\0'] = '\\0', ['\n'] = '\\n', ['\r'] = '\\r', ['\\'] = '\\\\',
    ["'"] = "\\'", ['"'] = '\\"', ['\026'] = '\\Z'
  }) .. "'"
end

-- Quotes an identifier for SQL
local function identifier(value)
  return '`' .. string.gsub(value, '`', '``') .. '`'
end

-- Authentication -------------------------------------------------------------

-- XOR of left with right, right repeated as needed
local function xor(left, right)
  local result = {}
  local size   = #right
  for index = 1, #left do
    result[index] = char(bxor(byte(left, index), byte(right, (index - 1) % size + 1)))
  end
  return table.concat(result)
end

-- mysql_native_password: SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
local function native(password, scramble)
  local first
  if password == '' then
    return ''
  end
  first = digest.sha1(password)
  return xor(first, digest.sha1(scramble .. digest.sha1(first)))
end

-- caching_sha2_password: SHA256(password) XOR SHA256(SHA256(SHA256(password)) + scramble)
local function sha2(password, scramble)
  local first
  if password == '' then
    return ''
  end
  first = digest.sha256(password)
  return xor(first, digest.sha256(digest.sha256(first) .. scramble))
end

-- Connection -----------------------------------------------------------------

local Connection   = {}
Connection.__index = Connection

local function server_error(packet)
  local data = reader(packet, 2)
  local code = data:u16()
  local message
  if byte(packet, 4) == 35 then -- '#' and SQLSTATE
    data:skip(6)
  end
  message = data:rest()
  return failure(message, code)
end

function Connection:read_packet(timeout)
  local parts, header, length, payload
  repeat
    header        = self.stream:read(4, timeout)
    length        = byte(header, 1) + byte(header, 2) * 0x100 + byte(header, 3) * 0x10000
    self.sequence = (byte(header, 4) + 1) % 0x100
    payload       = length > 0 and self.stream:read(length, timeout) or ''
    if length == 0xffffff or parts ~= nil then
      parts = parts or {}
      table.insert(parts, payload)
    end
  until length < 0xffffff
  if parts ~= nil then
    return table.concat(parts)
  end
  return payload
end

function Connection:write_packet(payload)
  local offset, length = 1, #payload
  repeat
    local size = math.min(length - offset + 1, 0xffffff)
    self.stream:write(sub(u32(size), 1, 3) .. char(self.sequence) .. sub(payload, offset, offset + size - 1))
    self.sequence = (self.sequence + 1) % 0x100
    offset        = offset + size
  until size < 0xffffff
end

function Connection:command(payload)
  self.sequence = 0
  if self.compressed then self.stream.sequence = 0 end
  self:write_packet(payload)
end

-- MirrorFlayer.Support for an authentication plugin, why tells what for
local function support(plugin, why)
  local status, module = pcall(require, 'MirrorFlayer.Support')
  if not status then
    fatal('%s needs MirrorFlayer.Support (Support.so) for %s: %s', plugin, why, tostring(module))
  end
  return module
end

-- Password for the full authentication of caching_sha2_password and
-- sha256_password without TLS: RSA-OAEP with the public key of the server
function Connection:encrypt(pem)
  return support(self.plugin, 'RSA without TLS').encrypt(pem, xor(self.password .. '\0', self.scramble))
end

-- dialog (PAM of MariaDB): a question is its type and its text; 4 and 5 ask
-- for the password, 2 and 3 for something else, an odd type is the last one
function Connection:answer(question)
  local kind = byte(question, 1) or 4
  if math.floor(kind / 2) ~= 2 then
    fatal('dialog asks %q, only a password can be answered', sub(question, 2))
  end
  return self.password .. '\0'
end

-- The largest iteration factor of parsec: PBKDF2 of 1024 << factor rounds
-- must fit into the timeout, as the client library of MariaDB estimates it
function Connection:factor()
  local rounds = 262144 / 225 * self.timeout * 1000
  return math.min(20, math.max(0, math.floor(math.log(rounds / 1024) / math.log(2))))
end

-- First answer of an authentication plugin to the scramble
function Connection:respond()
  local plugin, password = self.plugin, self.password
  if plugin == 'mysql_native_password' then
    return native(password, self.scramble)
  end
  if plugin == 'caching_sha2_password' then
    return sha2(password, self.scramble)
  end
  if plugin == 'sha256_password' then
    if password == '' then
      return ''
    end
    if self.secure then
      return password .. '\0'
    end
    if self.public_key ~= nil then
      return self:encrypt(self.public_key)
    end
    return '\1' -- asks for the public key
  end
  -- MariaDB: Ed25519 over the nonce with SHA-512 of the password as the key
  if plugin == 'client_ed25519' then
    return support(plugin, 'Ed25519').ed25519(password, self.challenge)
  end
  -- MariaDB 11.6 and later: the salt is asked with an empty packet first
  if plugin == 'parsec' then
    return ''
  end
  if plugin == 'mysql_clear_password' then
    return password .. '\0'
  end
  if plugin == 'dialog' then
    return self:answer(self.challenge)
  end
  fatal('authentication plugin %s is not supported', plugin)
end

-- AuthMoreData (0x01) of caching_sha2_password and sha256_password
function Connection:more(data)
  if self.plugin == 'caching_sha2_password' then
    if data == '\3' then
      return -- the server has the password cached, OK follows
    end
    if data == '\4' then -- full authentication
      if self.secure then
        self:write_packet(self.password .. '\0')
      elseif self.public_key ~= nil then
        self:write_packet(self:encrypt(self.public_key))
      else
        self:write_packet('\2') -- asks for the public key
      end
      return
    end
  end
  if self.plugin == 'caching_sha2_password' or self.plugin == 'sha256_password' then
    self:write_packet(self:encrypt(data)) -- the public key in PEM
    return
  end
  -- parsec: 'P', the iteration factor and 18 bytes of salt; the answer is a
  -- scramble of the client and the signature of both scrambles
  if self.plugin == 'parsec' then
    if #data ~= 20 or sub(data, 1, 1) ~= 'P' then
      fatal('parsec: unexpected parameters of %d bytes', #data)
    end
    local factor = byte(data, 2)
    if factor > self:factor() then
      fatal('parsec: the iteration factor %d does not fit into the timeout of %d s', factor, self.timeout)
    end
    local scramble = digest.urandom(32)
    self:write_packet(scramble .. support(self.plugin, 'Ed25519').parsec(self.password, sub(data, 3), factor,
      self.challenge .. scramble))
    return
  end
  if self.plugin == 'dialog' then
    self:write_packet(self:answer(data))
    return
  end
  fatal('unexpected authentication data for %s', self.plugin)
end

-- Greeting, SSL Request and TLS when asked, HandshakeResponse41, then the
-- authentication exchange
function Connection:handshake(user, database)
  local packet = self:read_packet()
  local data, capabilities, length, scramble, plugin, flags, response

  if byte(packet, 1) == 0xff then
    error(server_error(packet), 0)
  end

  data = reader(packet)
  if data:u8() ~= 10 then
    fatal('unsupported protocol version %d', byte(packet, 1))
  end
  self.version = data:cstring()
  self.mariadb = string.find(self.version, 'MariaDB', 1, true) ~= nil
  self.id      = data:u32()
  scramble     = data:bytes(8)
  data:skip(1)
  capabilities = data:u16()
  if data:left() > 0 then
    data:skip(3) -- character set and status
    capabilities = capabilities + data:u16() * 0x10000
    length       = data:u8()
    data:skip(10)
    if band(capabilities, CLIENT_SECURE_CONNECTION) ~= 0 then
      scramble = scramble .. data:bytes(math.max(13, length - 8))
    end
    if band(capabilities, CLIENT_PLUGIN_AUTH) ~= 0 then
      plugin = data:cstring()
    end
  end
  self.scramble     = sub(scramble, 1, 20)
  self.challenge    = self.scramble
  self.plugin       = plugin or 'mysql_native_password'
  self.capabilities = capabilities

  if band(capabilities, CLIENT_PROTOCOL_41) == 0 then
    fatal('server %s does not support protocol 4.1', self.version)
  end

  flags = bor(CLIENT_LONG_PASSWORD, CLIENT_LONG_FLAG, CLIENT_PROTOCOL_41,
    CLIENT_TRANSACTIONS, CLIENT_SECURE_CONNECTION, CLIENT_MULTI_RESULTS,
    CLIENT_PLUGIN_AUTH, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
  if database ~= nil then
    flags = bor(flags, CLIENT_CONNECT_WITH_DB)
  end
  flags = band(flags, bor(capabilities, CLIENT_PROTOCOL_41))

  -- Compression when the server has it and MirrorFlayer.Support is there to inflate
  if self.compress and band(capabilities, CLIENT_COMPRESS) ~= 0 then
    local status, support = pcall(require, 'MirrorFlayer.Support')
    if status then
      flags           = bor(flags, CLIENT_COMPRESS)
      self.uncompress = support.uncompress
    end
  end

  if self.tls ~= nil then
    if band(capabilities, CLIENT_SSL) == 0 then
      fatal('server %s does not support TLS', self.version)
    end
    flags = bor(flags, CLIENT_SSL)
    self:write_packet(u32(flags) .. u32(0x1000000) .. char(CHARSET_UTF8MB4_GENERAL_CI) .. string.rep('\0', 23))
    self.stream:secure(self.tls)
    self.secure = true
  end

  response = self:respond()
  if band(flags, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) ~= 0 then
    response = lenenc(#response) .. response
  else
    response = char(#response) .. response
  end

  self:write_packet(table.concat({
    u32(flags), u32(0x1000000), char(CHARSET_UTF8MB4_GENERAL_CI), string.rep('\0', 23),
    user, '\0', response,
    database ~= nil and database .. '\0' or '',
    band(flags, CLIENT_PLUGIN_AUTH) ~= 0 and self.plugin .. '\0' or ''
  }))

  while true do
    packet      = self:read_packet()
    local first = byte(packet, 1)
    if first == 0x00 then
      self.password = nil
      if self.uncompress ~= nil then
        self.stream     = setmetatable({ stream = self.stream, uncompress = self.uncompress, buffer = '', offset = 1, sequence = 0 }, Compressed)
        self.compressed = true
      end
      return
    end
    if first == 0xff then
      error(server_error(packet), 0)
    end
    if first == 0xfe then -- AuthSwitchRequest
      data           = reader(packet, 2)
      self.plugin    = data:cstring()
      scramble       = data:rest()
      self.challenge = scramble -- as it is: the nonce of Ed25519 may end with a zero byte
      if byte(scramble, #scramble) == 0 then
        scramble = sub(scramble, 1, -2)
      end
      self.scramble = scramble
      self:write_packet(self:respond())
    elseif first == 0x01 then -- AuthMoreData
      self:more(sub(packet, 2))
    else
      fatal('unexpected packet 0x%02x during authentication with %s', first, self.plugin)
    end
  end
end

-- Column definition of protocol 4.1
local function column_definition(packet)
  local data   = reader(packet)
  local column = {}
  data:lenstr() -- catalog
  column.schema = data:lenstr()
  column.table  = data:lenstr()
  data:lenstr() -- original table
  column.name = data:lenstr()
  data:lenstr() -- original name
  data:lenenc() -- length of fixed fields
  column.charset  = data:u16()
  column.length   = data:u32()
  column.type     = data:u8()
  column.flags    = data:u16()
  column.decimals = data:u8()
  return column
end

local function is_eof(packet)
  return byte(packet, 1) == 0xfe and #packet < 9
end

-- Runs a statement. For a result set calls each(row) for every row, where row
-- is an array of strings with box.NULL for NULL, and returns the column
-- definitions; without each the rows are collected and returned second.
-- For other statements returns nil and the affected row count.
function Connection:query(sql, each)
  local packet, first, count, columns, rows, data, row

  self:command(char(COM_QUERY) .. sql)
  packet = self:read_packet()
  first  = byte(packet, 1)

  if first == 0x00 then
    return nil, reader(packet, 2):lenenc()
  end
  if first == 0xff then
    error(server_error(packet), 0)
  end
  if first == 0xfb then
    fatal('LOCAL INFILE requested by the server')
  end

  count   = reader(packet):lenenc()
  columns = {}
  for index = 1, count do
    columns[index] = column_definition(self:read_packet())
  end
  packet = self:read_packet()
  if not is_eof(packet) then
    error(failure('expected EOF after column definitions'), 0)
  end

  if each == nil then
    rows = {}
    each = function (value) table.insert(rows, value) end
  end

  while true do
    packet = self:read_packet()
    if is_eof(packet) then
      break
    end
    if byte(packet, 1) == 0xff then
      error(server_error(packet), 0)
    end
    data = reader(packet)
    row  = {}
    for index = 1, count do
      row[index] = data:lenstr()
    end
    each(row)
  end

  return columns, rows
end

-- Returns the first column of the first row
function Connection:value(sql)
  local _, rows = self:query(sql)
  if rows[1] == nil then
    return nil
  end
  return rows[1][1]
end

function Connection:close()
  if self.stream ~= nil then
    pcall(function ()
      self:command(char(COM_QUIT))
    end)
    self.stream:close()
    self.stream = nil
  end
end

-- Opens a connection and authenticates.
-- options: host, port, user, password, database, timeout,
-- tls (nil or { verify, ca, cert, key }), public_key (PEM of the server for
-- caching_sha2_password and sha256_password without TLS), compress (the
-- compressed protocol when the server has it)
local function connect(options)
  local connection = setmetatable({
    sequence   = 0,
    password   = options.password or '',
    tls        = options.tls or nil,
    public_key = options.public_key,
    compress   = options.compress,
    timeout    = options.timeout or 30
  }, Connection)
  if connection.tls == true then
    connection.tls = { verify = 'none' }
  end
  connection.stream = transport.connect(options.host, options.port, options.timeout)
  local status, problem = pcall(connection.handshake, connection, options.user, options.database)
  if not status then
    connection.stream:close()
    connection.stream = nil
    error(problem, 0)
  end
  return connection
end

return {
  NULL         = NULL,
  CLIENT_SSL   = CLIENT_SSL,
  connect      = connect,
  reader       = reader,
  failure      = failure,
  fatal        = fatal,
  server_error = server_error,
  literal      = literal,
  identifier   = identifier,
  u16          = u16,
  u32          = u32,
  u64          = u64,
  lenenc       = lenenc
}
