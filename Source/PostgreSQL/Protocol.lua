-- PostgreSQL frontend/backend protocol 3.0: messages, SSLRequest and TLS on
-- the connected socket, authentication (SCRAM-SHA-256, md5, cleartext), the
-- simple query protocol with a streamed result and CopyBoth for replication

local ffi    = require('ffi')
local bit    = require('bit')
local digest = require('digest')
local crypto = require('crypto')

local transport = require('MirrorFlayer.Transport')
local errors    = require('MirrorFlayer.Failure')

local byte, char, sub = string.byte, string.char, string.sub

local failure, fatal = errors.failure, errors.fatal

local NULL = box.NULL

local PROTOCOL    = 196608   -- 3.0
local SSL_REQUEST = 80877103

local AUTH_OK         = 0
local AUTH_CLEARTEXT  = 3
local AUTH_MD5        = 5
local AUTH_SASL       = 10
local AUTH_SASL_NEXT  = 11
local AUTH_SASL_FINAL = 12

-- Reader over a string, big-endian -----------------------------------------------

local Reader   = {}
Reader.__index = Reader

local function reader(data)
  return setmetatable({ data = data, position = 1 }, Reader)
end

function Reader:left()
  return #self.data - self.position + 1
end

function Reader:check(n)
  if self:left() < n then
    fatal('message is truncated')
  end
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
  return a * 0x100 + b
end

function Reader:i16()
  local value = self:u16()
  return value >= 0x8000 and value - 0x10000 or value
end

function Reader:u32()
  self:check(4)
  local a, b, c, d = byte(self.data, self.position, self.position + 3)
  self.position = self.position + 4
  return ((a * 0x100 + b) * 0x100 + c) * 0x100 + d
end

function Reader:i32()
  local value = self:u32()
  return value >= 0x80000000 and value - 0x100000000 or value
end

-- An unsigned 64-bit value (an LSN) as uint64_t
function Reader:u64()
  local high = self:u32()
  local low  = self:u32()
  return ffi.cast('uint64_t', high) * 0x100000000ULL + low
end

-- A signed 64-bit value (microseconds) as a number
function Reader:i64()
  local high = self:u32()
  local low  = self:u32()
  if high >= 0x80000000 then
    high = high - 0x100000000
  end
  return high * 0x100000000 + low
end

function Reader:bytes(n)
  self:check(n)
  local value   = sub(self.data, self.position, self.position + n - 1)
  self.position = self.position + n
  return value
end

function Reader:cstring()
  local stop = string.find(self.data, '\0', self.position, true)
  if stop == nil then
    fatal('string without its end')
  end
  local value   = sub(self.data, self.position, stop - 1)
  self.position = stop + 1
  return value
end

function Reader:rest()
  local value   = sub(self.data, self.position)
  self.position = #self.data + 1
  return value
end

-- Writing ------------------------------------------------------------------------

local function i16(value)
  return char(bit.band(bit.rshift(value, 8), 0xff), bit.band(value, 0xff))
end

local function i32(value)
  value = bit.tobit(value)
  return char(bit.band(bit.rshift(value, 24), 0xff), bit.band(bit.rshift(value, 16), 0xff),
    bit.band(bit.rshift(value, 8), 0xff), bit.band(value, 0xff))
end

-- An unsigned 64-bit value given as uint64_t or a number
local function u64(value)
  value = ffi.cast('uint64_t', value)
  return i32(tonumber(value / 0x100000000ULL)) .. i32(tonumber(value % 0x100000000ULL))
end

-- A signed 64-bit number
local function i64(value)
  local high = math.floor(value / 0x100000000)
  return i32(high) .. i32(value - high * 0x100000000)
end

local function literal(value)
  return "'" .. string.gsub(tostring(value), "'", "''") .. "'"
end

local function identifier(value)
  return '"' .. string.gsub(tostring(value), '"', '""') .. '"'
end

-- Errors -------------------------------------------------------------------------

-- ErrorResponse: fields by their code; the failure has the SQLSTATE as code
local function server_error(payload)
  local data   = reader(payload)
  local fields = {}
  while data:left() > 0 do
    local code = data:u8()
    if code == 0 then
      break
    end
    fields[char(code)] = data:cstring()
  end
  local problem  = failure(string.format('%s: %s', fields.S or 'ERROR', fields.M or 'unknown error'), fields.C)
  problem.detail = fields.D
  return problem
end

-- SCRAM-SHA-256 --------------------------------------------------------------------

local function xor(left, right)
  local result = {}
  for index = 1, #left do
    result[index] = char(bit.bxor(byte(left, index), byte(right, index)))
  end
  return table.concat(result)
end

local function base64(data)
  return digest.base64_encode(data, { nowrap = true })
end

local function attributes(text)
  local result = {}
  for name, value in string.gmatch(text, '(%a)=([^,]*)') do
    result[name] = value
  end
  return result
end

-- Connection ---------------------------------------------------------------------

local Connection   = {}
Connection.__index = Connection

-- Reads one message: its type (a character) and payload
function Connection:receive(timeout)
  local header = self.stream:read(5, timeout)
  local length = reader(sub(header, 2)):u32()
  if length < 4 then
    fatal('bad message length %d', length)
  end
  return sub(header, 1, 1), length > 4 and self.stream:read(length - 4) or ''
end

function Connection:send(kind, payload)
  self.stream:write(kind .. i32(#payload + 4) .. payload)
end

-- Waits up to timeout for a message without reading it
function Connection:wait(timeout)
  return self.stream:wait(timeout)
end

function Connection:password(payload)
  self:send('p', payload)
end

function Connection:scram(mechanisms)
  local found = false
  for mechanism in string.gmatch(mechanisms, '[^%z]+') do
    if mechanism == 'SCRAM-SHA-256' then
      found = true
    end
  end
  if not found then
    fatal('the server offers no SCRAM-SHA-256: %s', string.gsub(mechanisms, '%z', ' '))
  end
  local nonce = base64(digest.urandom(18))
  local first = 'n=,r=' .. nonce
  self:password('SCRAM-SHA-256\0' .. i32(#first + 3) .. 'n,,' .. first)

  local kind, payload = self:receive()
  if kind == 'E' then
    error(server_error(payload), 0)
  end
  local data = reader(payload)
  if kind ~= 'R' or data:u32() ~= AUTH_SASL_NEXT then
    fatal('unexpected message %q during SCRAM', kind)
  end
  local server = data:rest()
  local values = attributes(server)
  if values.r == nil or sub(values.r, 1, #nonce) ~= nonce or values.s == nil or tonumber(values.i) == nil then
    fatal('bad SCRAM challenge')
  end
  local salted    = digest.pbkdf2(self.options.password or '', digest.base64_decode(values.s), tonumber(values.i), 32)
  local client    = crypto.hmac.sha256(salted, 'Client Key')
  local stored    = digest.sha256(client)
  local final     = 'c=biws,r=' .. values.r
  local message   = first .. ',' .. server .. ',' .. final
  local signature = crypto.hmac.sha256(stored, message)
  self:password(final .. ',p=' .. base64(xor(client, signature)))

  kind, payload = self:receive()
  if kind == 'E' then
    error(server_error(payload), 0)
  end
  data = reader(payload)
  if kind ~= 'R' or data:u32() ~= AUTH_SASL_FINAL then
    fatal('unexpected message %q at the end of SCRAM', kind)
  end
  local expected = crypto.hmac.sha256(crypto.hmac.sha256(salted, 'Server Key'), message)
  if attributes(data:rest()).v ~= base64(expected) then
    fatal('the server failed to prove it knows the password')
  end
end

function Connection:authenticate()
  while true do
    local kind, payload = self:receive()
    if kind == 'E' then
      error(server_error(payload), 0)
    end
    if kind == 'R' then
      local data = reader(payload)
      local code = data:u32()
      if code == AUTH_OK then
        return
      elseif code == AUTH_CLEARTEXT then
        self:password((self.options.password or '') .. '\0')
      elseif code == AUTH_MD5 then
        local salt  = data:bytes(4)
        local inner = digest.md5_hex((self.options.password or '') .. self.options.user)
        self:password('md5' .. digest.md5_hex(inner .. salt) .. '\0')
      elseif code == AUTH_SASL then
        self:scram(data:rest())
      else
        fatal('authentication method %d is not supported', code)
      end
    elseif kind ~= 'v' and kind ~= 'N' then
      fatal('unexpected message %q during authentication', kind)
    end
  end
end

-- Reads until ReadyForQuery after the authentication
function Connection:ready()
  while true do
    local kind, payload = self:receive()
    if kind == 'Z' then
      return
    elseif kind == 'S' then
      local data            = reader(payload)
      local name            = data:cstring()
      self.parameters[name] = data:cstring()
    elseif kind == 'E' then
      error(server_error(payload), 0)
    end
    -- BackendKeyData, NoticeResponse
  end
end

-- Runs SQL with the simple query protocol. each(row) gets every row as it
-- comes (text values or box.NULL); without it the rows are returned. Returns
-- the names of the columns, the rows and the command tag of the last result.
-- A copy started by the statement (START_REPLICATION) returns with copy set.
function Connection:query(sql, each)
  self:send('Q', sql .. '\0')
  local names, rows, tag, problem = {}, {}, nil, nil
  while true do
    local kind, payload = self:receive()
    if kind == 'D' then
      local data  = reader(payload)
      local count = data:u16()
      local row   = {}
      for index = 1, count do
        local length = data:i32()
        row[index]   = length < 0 and NULL or data:bytes(length)
      end
      if each ~= nil then
        each(row)
      else
        table.insert(rows, row)
      end
    elseif kind == 'T' then
      local data  = reader(payload)
      local count = data:u16()
      names       = {}
      for index = 1, count do
        names[index] = data:cstring()
        data:bytes(18) -- table, column, type, size, modifier, format
      end
    elseif kind == 'C' then
      tag = reader(payload):cstring()
    elseif kind == 'E' then
      problem = server_error(payload)
    elseif kind == 'Z' then
      if problem ~= nil then
        error(problem, 0)
      end
      return names, rows, tag
    elseif kind == 'W' or kind == 'H' then
      return names, rows, tag, true
    elseif kind == 'S' then
      local data            = reader(payload)
      local name            = data:cstring()
      self.parameters[name] = data:cstring()
    end
    -- EmptyQueryResponse, NoticeResponse, NotificationResponse
  end
end

-- The first value of the first row
function Connection:value(sql)
  local _, rows = self:query(sql)
  if #rows == 0 then
    return nil
  end
  return rows[1][1]
end

function Connection:close()
  if self.stream ~= nil then
    pcall(self.send, self, 'X', '')
    self.stream:close()
    self.stream = nil
  end
end

-- Connects and authenticates. options: host, port, user, password, database,
-- timeout, tls (nil or { verify, ca, cert, key }), replication (true for a
-- logical replication connection), parameters (more startup parameters)
local function connect(options)
  local stream = transport.connect(options.host, options.port, options.timeout)
  local self   = setmetatable({ stream = stream, options = options, parameters = {} }, Connection)
  local status, problem = pcall(function ()
    if options.tls ~= nil then
      stream:write(i32(8) .. i32(SSL_REQUEST))
      local answer = stream:read(1)
      if answer ~= 'S' then
        fatal('server %s:%d does not support TLS', options.host, options.port)
      end
      stream:secure(options.tls)
      self.secure = true
    end
    local parameters = {
      user               = options.user,
      database           = options.database or options.user,
      application_name   = 'MirrorFlayer',
      client_encoding    = 'UTF8',
      DateStyle          = 'ISO',
      TimeZone           = 'UTC',
      IntervalStyle      = 'iso_8601',
      extra_float_digits = '3',
      bytea_output       = 'hex'
    }
    if options.replication then
      parameters.replication = 'database'
    end
    for name, value in pairs(options.parameters or {}) do
      parameters[name] = value
    end
    local items = {}
    for name, value in pairs(parameters) do
      table.insert(items, name .. '\0' .. tostring(value) .. '\0')
    end
    local startup = i32(PROTOCOL) .. table.concat(items) .. '\0'
    stream:write(i32(#startup + 4) .. startup)
    self:authenticate()
    self:ready()
  end)
  if not status then
    stream:close()
    error(problem, 0)
  end
  self.version = 'PostgreSQL ' .. tostring(self.parameters.server_version)
  return self
end

return {
  connect      = connect,
  reader       = reader,
  server_error = server_error,
  literal      = literal,
  identifier   = identifier,
  i32          = i32,
  i64          = i64,
  u64          = u64
}
