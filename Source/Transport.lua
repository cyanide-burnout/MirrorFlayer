-- Byte stream to the server: read(n), write(s), close()
--
-- Plain TCP uses the socket module of Tarantool. TLS (MirrorFlayer.TLS) takes
-- over the descriptor of an already connected socket, because MySQL switches
-- to TLS after the greeting on the same connection: secure() is called after
-- the SSL Request, then reads and writes go through the TLS session.

local socket = require('socket')

local Transport = {}
Transport.__index = Transport

local function connect(host, port, timeout)
  local sock, problem = socket.tcp_connect(host, port, timeout)
  if sock == nil then
    error(string.format('connect to %s:%d: %s', host, port, tostring(problem or socket.strerror and socket.strerror() or 'failed')), 0)
  end
  sock:setsockopt('SOL_SOCKET', 'SO_KEEPALIVE', true)
  return setmetatable({ sock = sock, timeout = timeout, host = host, peer = host .. ':' .. port }, Transport)
end

-- Starts TLS on the connected socket.
-- options: verify ('none' | 'ca' | 'identity'), ca, cert, key
function Transport:secure(options)
  local status, TLS = pcall(require, 'MirrorFlayer.TLS')
  if not status then
    error(setmetatable({ message = 'TLS is requested, but MirrorFlayer.TLS (TLS.so) cannot be loaded: ' .. tostring(TLS), fatal = true },
      { __tostring = function (self) return self.message end }), 0)
  end
  self.session = TLS.connect(self.sock:fd(), {
    host    = self.host,
    verify  = options.verify,
    ca      = options.ca,
    cert    = options.cert,
    key     = options.key,
    timeout = self.timeout
  })
end

-- Reads exactly n bytes or raises
function Transport:read(n, timeout)
  if self.session ~= nil then
    return self.session:read(n, timeout or self.timeout)
  end
  local data = self.sock:read(n, timeout or self.timeout)
  if data == nil then
    error(string.format('read from %s: %s', self.peer, tostring(self.sock:error())), 0)
  end
  if #data < n then
    error(string.format('connection to %s closed by peer', self.peer), 0)
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
