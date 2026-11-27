-- Byte stream to the server: read(n), write(s), wait(timeout), close()
--
-- Plain TCP uses the socket module of Tarantool. TLS (MirrorFlayer.Support) takes
-- over the descriptor of an already connected socket, because MySQL and
-- PostgreSQL switch to TLS on the same connection: secure() is called after
-- the SSL Request, then reads and writes go through the TLS session.

local fiber  = require('fiber')
local errno  = require('errno')
local socket = require('socket')

-- Plain TCP is read without the read-ahead buffer of the socket module, into
-- a buffer of the transport, so wait() knows whether data is already there
local CHUNK = 65536

local Transport   = {}
Transport.__index = Transport

local function connect(host, port, timeout)
  local sock, problem = socket.tcp_connect(host, port, timeout)
  if sock == nil then
    error(string.format('connect to %s:%d: %s', host, port, tostring(problem or socket.strerror and socket.strerror() or 'failed')), 0)
  end
  sock:setsockopt('SOL_SOCKET', 'SO_KEEPALIVE', true)
  sock:nonblock(true)
  return setmetatable({ sock = sock, timeout = timeout, host = host, peer = host .. ':' .. port,
    buffer = '', offset = 1 }, Transport)
end

-- Starts TLS on the connected socket.
-- options: verify ('none' | 'ca' | 'identity'), ca, cert, key
function Transport:secure(options)
  if self:buffered() > 0 then
    error(string.format('%s sent data before the TLS handshake', self.peer), 0)
  end
  local status, support = pcall(require, 'MirrorFlayer.Support')
  if not status then
    error(setmetatable({ message = 'TLS is requested, but MirrorFlayer.Support (Support.so) cannot be loaded: ' .. tostring(support), fatal = true },
      { __tostring = function (self) return self.message end }), 0)
  end
  self.session = support.connect(self.sock:fd(), {
    host    = self.host,
    verify  = options.verify,
    ca      = options.ca,
    cert    = options.cert,
    key     = options.key,
    timeout = self.timeout
  })
end

-- Bytes in the buffer, not read yet
function Transport:buffered()
  return #self.buffer - self.offset + 1
end

-- Reads what the socket has, waiting up to timeout; nil when it had nothing
-- after all
function Transport:receive(timeout)
  if not self.sock:readable(timeout) then
    error(string.format('read from %s: timed out', self.peer), 0)
  end
  local data = self.sock:sysread(CHUNK)
  if data == nil then
    local code = self.sock:errno()
    if code == errno.EAGAIN or code == errno.EWOULDBLOCK or code == errno.EINTR then
      return nil
    end
    error(string.format('read from %s: %s', self.peer, tostring(self.sock:error())), 0)
  end
  if data == '' then
    error(string.format('connection to %s closed by peer', self.peer), 0)
  end
  return data
end

-- Reads exactly n bytes or raises
function Transport:read(n, timeout)
  if self.session ~= nil then
    return self.session:read(n, timeout or self.timeout)
  end
  local available = self:buffered()
  if available < n then
    -- Chunks are joined once, a large packet is not copied over and over
    local deadline = fiber.clock() + (timeout or self.timeout)
    local parts    = { string.sub(self.buffer, self.offset) }
    while available < n do
      local data = self:receive(math.max(0, deadline - fiber.clock()))
      if data ~= nil then
        table.insert(parts, data)
        available = available + #data
      end
    end
    self.buffer, self.offset = table.concat(parts), 1
  end
  local data  = string.sub(self.buffer, self.offset, self.offset + n - 1)
  self.offset = self.offset + n
  if self.offset > #self.buffer then
    self.buffer, self.offset = '', 1
  end
  return data
end

function Transport:write(data, timeout)
  if self.session ~= nil then
    self.session:write(data, timeout or self.timeout)
    return
  end
  local sent = self.sock:write(data, timeout or self.timeout)
  if sent == nil then
    error(string.format('write to %s: %s', self.peer, tostring(self.sock:error())), 0)
  end
end

-- Waits up to timeout for data to read without reading it; false when the
-- time ran out
function Transport:wait(timeout)
  if self.session ~= nil then
    return self.session:wait(timeout)
  end
  return self:buffered() > 0 or self.sock:readable(timeout) == true
end

function Transport:close()
  if self.session ~= nil then
    self.session:close()
    self.session = nil
  end
  if self.sock ~= nil then
    self.sock:close()
    self.sock = nil
  end
end

function Transport:fd()
  return self.sock:fd()
end

return { connect = connect }
