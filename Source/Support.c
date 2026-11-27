// MirrorFlayer.Support: TLS (OpenSSL) over an already connected descriptor, RSA-OAEP for caching_sha2_password
// and inflate (zlib) for the compressed protocol and the compressed binlog events of MySQL and MariaDB
//
// MySQL switches to TLS after the greeting on the same TCP connection (the SSL Request packet), so a session takes
// over the descriptor of the Tarantool socket, sock:fd(). The descriptor stays non-blocking and belongs to the socket,
// which closes it; the session waits with coio_wait() on SSL_ERROR_WANT_READ / SSL_ERROR_WANT_WRITE, so other fibers
// run meanwhile.
//
//   local session = Support.connect(sock:fd(), { host = 'db1', verify = 'identity', ca = '/etc/ssl/ca.pem',
//                                            cert = 'client.pem', key = 'client.key', timeout = 30 })
//   session:read(n [, timeout])        -- exactly n bytes
//   session:write(data [, timeout])
//   session:wait(timeout)              -- true when data can be read, false when the time ran out
//   session:close()
//   Support.encrypt(pem, data)         -- RSA-OAEP (SHA-1, as MySQL) with a PEM public key
//   Support.uncompress(data, length)   -- data of compress() of zlib, length is the size of the original
//
// verify is 'none' (encryption only), 'ca' (the chain against ca or the system store) or 'identity' (also the host
// name). Errors are raised with the reason of OpenSSL.
//
// tarantool exports no OpenSSL symbols (its OpenSSL is linked in statically and hidden), so this module brings its
// own: the copies do not see each other and never share objects.

#include <errno.h>
#include <string.h>
#include <zlib.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/x509v3.h>

#include <lua.h>
#include <lauxlib.h>
#include <module.h>

#define SESSION  "MirrorFlayer.Support"
#define CHUNK    16384
#define TIMEOUT  30.0

struct Session
{
  SSL_CTX* context;
  SSL* connection;
  int handle;
  double timeout;
};

// Raises the first error of OpenSSL, the queue is cleared

static int RaiseError(lua_State* state, const char* action)
{
  char buffer[256];
  unsigned long code;

  code = ERR_get_error();
  ERR_clear_error();

  if (code == 0)
    return luaL_error(state, "TLS %s failed", action);

  ERR_error_string_n(code, buffer, sizeof(buffer));
  return luaL_error(state, "TLS %s: %s", action, buffer);
}

// Waits for the descriptor after a failed call of SSL_connect(), SSL_read() or SSL_write(); raises on other errors,
// on the deadline (fiber_clock()) and on cancellation

static void WaitSession(lua_State* state, struct Session* session, int result, double deadline, const char* action)
{
  int error;
  int events;
  double left;

  error  = SSL_get_error(session->connection, result);
  events = 0;

  switch (error)
  {
    case SSL_ERROR_WANT_READ:
      events = COIO_READ;
      break;

    case SSL_ERROR_WANT_WRITE:
      events = COIO_WRITE;
      break;

    case SSL_ERROR_ZERO_RETURN:
      luaL_error(state, "TLS %s: connection closed by peer", action);
      return;

    case SSL_ERROR_SYSCALL:
      if (ERR_peek_error() == 0)
      {
        luaL_error(state, "TLS %s: %s", action, ((result == 0) || (errno == 0)) ? "connection closed by peer" : strerror(errno));
        return;
      }
      RaiseError(state, action);
      return;

    default:
      RaiseError(state, action);
      return;
  }

  // A wait that runs out comes back here with no time left

  left = deadline - fiber_clock();

  if (left <= 0.0)
    luaL_error(state, "TLS %s: timed out", action);

  coio_wait(session->handle, events, left);

  if (fiber_is_cancelled())
    luaL_error(state, "fiber is cancelled");
}

static void ReleaseSession(struct Session* session)
{
  if (session->connection != NULL)
  {
    SSL_shutdown(session->connection);
    SSL_free(session->connection);
    session->connection = NULL;
  }

  if (session->context != NULL)
  {
    SSL_CTX_free(session->context);
    session->context = NULL;
  }

  ERR_clear_error();
}

static struct Session* CheckSession(lua_State* state)
{
  struct Session* session;

  session = (struct Session*)luaL_checkudata(state, 1, SESSION);

  if (session->connection == NULL)
    luaL_error(state, "TLS session is closed");

  return session;
}

static const char* GetOption(lua_State* state, const char* name)
{
  const char* value;

  lua_getfield(state, 2, name);
  value = (lua_type(state, -1) == LUA_TSTRING) ? lua_tostring(state, -1) : NULL;
  lua_pop(state, 1);

  // The string stays referenced by the table of options
  return value;
}

// Support.connect(descriptor, options): handshake over the connected descriptor

static int ConnectSession(lua_State* state)
{
  struct Session* session;
  const char* host;
  const char* verify;
  const char* authority;
  const char* certificate;
  const char* key;
  double deadline;
  int result;

  luaL_checkinteger(state, 1);
  luaL_checktype(state, 2, LUA_TTABLE);

  host        = GetOption(state, "host");
  verify      = GetOption(state, "verify");
  authority   = GetOption(state, "ca");
  certificate = GetOption(state, "cert");
  key         = GetOption(state, "key");

  if (verify == NULL)
    verify = "none";

  if ((strcmp(verify, "none") != 0) &&
      (strcmp(verify, "ca") != 0) &&
      (strcmp(verify, "identity") != 0))
    return luaL_error(state, "TLS: verify must be 'none', 'ca' or 'identity'");

  if ((strcmp(verify, "identity") == 0) && (host == NULL))
    return luaL_error(state, "TLS: verify = 'identity' needs the host");

  session = (struct Session*)lua_newuserdata(state, sizeof(struct Session));
  memset(session, 0, sizeof(struct Session));
  luaL_getmetatable(state, SESSION);
  lua_setmetatable(state, -2);

  session->handle = lua_tointeger(state, 1);
  lua_getfield(state, 2, "timeout");
  session->timeout = luaL_optnumber(state, -1, TIMEOUT);
  lua_pop(state, 1);

  ERR_clear_error();

  if ((session->context = SSL_CTX_new(TLS_client_method())) == NULL)
    return RaiseError(state, "context");

  SSL_CTX_set_min_proto_version(session->context, TLS1_2_VERSION);

  if (strcmp(verify, "none") == 0)
    SSL_CTX_set_verify(session->context, SSL_VERIFY_NONE, NULL);
  else
  {
    SSL_CTX_set_verify(session->context, SSL_VERIFY_PEER, NULL);

    if (((authority != NULL) && (SSL_CTX_load_verify_locations(session->context, authority, NULL) != 1)) ||
        ((authority == NULL) && (SSL_CTX_set_default_verify_paths(session->context) != 1)))
      return RaiseError(state, "certificate authority");
  }

  if ((certificate != NULL) &&
      ((SSL_CTX_use_certificate_chain_file(session->context, certificate) != 1) ||
       (SSL_CTX_use_PrivateKey_file(session->context, (key != NULL) ? key : certificate, SSL_FILETYPE_PEM) != 1)))
    return RaiseError(state, "client certificate");

  if (((session->connection = SSL_new(session->context)) == NULL) ||
      (SSL_set_fd(session->connection, session->handle) != 1))
    return RaiseError(state, "session");

  SSL_set_mode(session->connection, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

  if ((host != NULL) &&
      (strchr(host, ':') == NULL) &&
      (strspn(host, "0123456789.") != strlen(host)))
    SSL_set_tlsext_host_name(session->connection, host);

  if ((strcmp(verify, "identity") == 0) &&
      (SSL_set1_host(session->connection, host) != 1))
    return RaiseError(state, "host name");

  deadline = fiber_clock() + session->timeout;

  while ((result = SSL_connect(session->connection)) != 1)
    WaitSession(state, session, result, deadline, "handshake");

  return 1;
}

// session:read(n [, timeout]): exactly n bytes

static int ReadSession(lua_State* state)
{
  struct Session* session;
  luaL_Buffer output;
  char buffer[CHUNK];
  size_t length;
  size_t done;
  double deadline;
  int result;

  session  = CheckSession(state);
  length   = (size_t)luaL_checkinteger(state, 2);
  deadline = fiber_clock() + luaL_optnumber(state, 3, session->timeout);
  done     = 0;

  luaL_buffinit(state, &output);

  while (done < length)
  {
    result = SSL_read(session->connection, buffer, (int)(((length - done) < CHUNK) ? (length - done) : CHUNK));

    if (result > 0)
    {
      luaL_addlstring(&output, buffer, result);
      done += result;
      continue;
    }

    WaitSession(state, session, result, deadline, "read");
  }

  luaL_pushresult(&output);
  return 1;
}

// session:write(data [, timeout])

static int WriteSession(lua_State* state)
{
  struct Session* session;
  const char* data;
  size_t length;
  size_t done;
  double deadline;
  int result;

  session  = CheckSession(state);
  data     = luaL_checklstring(state, 2, &length);
  deadline = fiber_clock() + luaL_optnumber(state, 3, session->timeout);
  done     = 0;

  while (done < length)
  {
    result = SSL_write(session->connection, data + done, (int)(length - done));

    if (result > 0)
    {
      done += result;
      continue;
    }

    WaitSession(state, session, result, deadline, "write");
  }

  return 0;
}

// session:wait(timeout): waits for data without reading it, so a stream can send something of its own while the
// other side is silent; decrypted data already buffered counts

static int WaitData(lua_State* state)
{
  struct Session* session;
  double timeout;
  int events;

  session = CheckSession(state);
  timeout = luaL_optnumber(state, 2, session->timeout);

  if (SSL_pending(session->connection) > 0)
  {
    lua_pushboolean(state, 1);
    return 1;
  }

  events = coio_wait(session->handle, COIO_READ, timeout);

  if (fiber_is_cancelled())
    luaL_error(state, "fiber is cancelled");

  lua_pushboolean(state, events != 0);
  return 1;
}

static int CloseSession(lua_State* state)
{
  struct Session* session;

  session = (struct Session*)luaL_checkudata(state, 1, SESSION);
  ReleaseSession(session);
  return 0;
}

// Support.encrypt(pem, data): RSA with OAEP padding (SHA-1 and MGF1 with SHA-1), what caching_sha2_password and
// sha256_password of MySQL expect from a client without TLS

static int EncryptData(lua_State* state)
{
  const char* text;
  const char* data;
  size_t size;
  size_t length;
  BIO* input;
  EVP_PKEY* key;
  EVP_PKEY_CTX* context;
  unsigned char buffer[1024];
  int result;

  text = luaL_checklstring(state, 1, &size);
  data = luaL_checklstring(state, 2, &length);

  ERR_clear_error();

  if ((input = BIO_new_mem_buf(text, (int)size)) == NULL)
    return RaiseError(state, "public key");

  key = PEM_read_bio_PUBKEY(input, NULL, NULL, NULL);
  BIO_free(input);

  if (key == NULL)
    return RaiseError(state, "public key");

  if (EVP_PKEY_get_size(key) > (int)sizeof(buffer))
  {
    EVP_PKEY_free(key);
    return luaL_error(state, "TLS encrypt: the key is too large");
  }

  size    = sizeof(buffer);
  context = EVP_PKEY_CTX_new(key, NULL);
  result  =
    (context != NULL) &&
    (EVP_PKEY_encrypt_init(context) > 0) &&
    (EVP_PKEY_CTX_set_rsa_padding(context, RSA_PKCS1_OAEP_PADDING) > 0) &&
    (EVP_PKEY_encrypt(context, buffer, &size, (const unsigned char*)data, length) > 0);

  EVP_PKEY_CTX_free(context);
  EVP_PKEY_free(key);

  if (!result)
    return RaiseError(state, "encrypt");

  lua_pushlstring(state, (const char*)buffer, size);
  return 1;
}

// Support.uncompress(data, length): what compress() of zlib made of length bytes, the frames of the compressed
// protocol and the compressed binlog events carry the length of the original; the buffer is a userdata of Lua

static int UncompressData(lua_State* state)
{
  const char* data;
  size_t size;
  lua_Number expected;
  uLongf length;
  Bytef* buffer;
  int result;

  data     = luaL_checklstring(state, 1, &size);
  expected = luaL_checknumber(state, 2);

  if ((expected < 1) || (expected > 0x40000000))
    return luaL_error(state, "uncompress: the length %f is out of range", expected);

  length = (uLongf)expected;
  buffer = (Bytef*)lua_newuserdata(state, length);
  result = uncompress(buffer, &length, (const Bytef*)data, (uLong)size);

  if ((result != Z_OK) || (length != (uLongf)expected))
    return luaL_error(state, "uncompress: %s", (result != Z_OK) ? zError(result) : "the length differs");

  lua_pushlstring(state, (const char*)buffer, length);
  return 1;
}

LUA_API int luaopen_MirrorFlayer_Support(lua_State* state)
{
  static const luaL_Reg methods[] =
  {
    { "read",  ReadSession  },
    { "write", WriteSession },
    { "wait",  WaitData     },
    { "close", CloseSession },
    { "__gc",  CloseSession },
    { NULL,    NULL         }
  };

  static const luaL_Reg functions[] =
  {
    { "connect",    ConnectSession },
    { "encrypt",    EncryptData    },
    { "uncompress", UncompressData },
    { NULL,         NULL           }
  };

  OPENSSL_init_ssl(0, NULL);

  luaL_newmetatable(state, SESSION);
  lua_pushvalue(state, -1);
  lua_setfield(state, -2, "__index");
  luaL_register(state, NULL, methods);
  lua_pop(state, 1);

  lua_newtable(state);
  luaL_register(state, NULL, functions);
  lua_pushstring(state, OpenSSL_version(OPENSSL_VERSION));
  lua_setfield(state, -2, "version");
  return 1;
}
