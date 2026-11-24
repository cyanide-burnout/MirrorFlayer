-- Byte stream to the server: read(n), write(s), close()
--
-- Plain TCP uses the socket module of Tarantool. TLS (mirrorflayer.tls) takes
-- over the descriptor of an already connected socket, because MySQL switches
-- to TLS after the greeting on the same connection; it is not available yet.

local socket = require('socket')

local Transport = {}
Transport.__index = Transport

local function connect(host, port, timeout)
  local sock, problem = socket.tcp_connect(host, port, timeout)
  if sock == nil then
    error(string.format('connect to %s:%d: %s', host, port, tostring(problem or socket.strerror and socket.strerror() or 'failed')), 0)
  end
  sock:setsockopt('SOL_SOCKET', 'SO_KEEPALIVE', true)
  return setmetatable({ sock = sock, timeout = timeout, peer = host .. ':' .. port }, Transport)
end

-- Reads exactly n bytes or raises
function Transport:read(n, timeout)
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
  local sent = self.sock:write(data, timeout or self.timeout)
  if sent == nil then
    error(string.format('write to %s: %s', self.peer, tostring(self.sock:error())), 0)
  end
end

function Transport:close()
  if self.sock ~= nil then
    self.sock:close()
    self.sock = nil
  end
end

function Transport:fd()
  return self.sock:fd()
end

return { connect = connect }
