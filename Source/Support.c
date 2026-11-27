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
//   Support.crc32(data [, length])     -- CRC-32 of zlib (IEEE 802.3) of the first length bytes
//   Support.ed25519(password, message) -- the signature of client_ed25519 of MariaDB
//   Support.parsec(password, salt, factor, message)
//                                      -- the signature of parsec of MariaDB
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

// Support.crc32(data [, length]): CRC-32 of zlib (IEEE 802.3) of the first length bytes, the checksum of binlog events

static int CalculateChecksum(lua_State* state)
{
  const char* data;
  size_t size;
  lua_Integer length;

  data   = luaL_checklstring(state, 1, &size);
  length = luaL_optinteger(state, 2, (lua_Integer)size);

  if ((length < 0) || ((size_t)length > size))
    return luaL_error(state, "crc32: the length is out of range");

  lua_pushnumber(state, (lua_Number)crc32(0L, (const Bytef*)data, (uInt)length));
  return 1;
}

// Ed25519 of client_ed25519: MariaDB signs with the expanded key SHA-512 of the password itself, not of a 32-byte seed,
// which EVP of OpenSSL cannot take, so the group operations of edwards25519 are done here over BIGNUM: extended
// coordinates (X:Y:Z:T), the unified addition of a = -1 (add-2008-hwcd-3). Not in constant time: one signature per
// connection

#define ED25519_P   "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffed"
#define ED25519_L   "1000000000000000000000000000000014def9dea2f79cd65812631a5cf5d3ed"
#define ED25519_D   "52036cee2b6ffe738cc740797779e89800700a4d4141d8ab75eb4dca135978a3"
#define ED25519_BX  "216936d3cd6e53fec0a4e231fdd6dc5c692cc7609525a7b2c9562d608f25d51a"
#define ED25519_BY  "6666666666666666666666666666666666666666666666666666666666666658"

struct Point
{
  BIGNUM* x;
  BIGNUM* y;
  BIGNUM* z;
  BIGNUM* t;
};

struct Curve
{
  BN_CTX* context;
  BIGNUM* p;
  BIGNUM* l;
  BIGNUM* d2;
  struct Point base;
};

static int AddPoints(struct Curve* curve, struct Point* result, const struct Point* left, const struct Point* right)
{
  BN_CTX* context;
  BIGNUM* a;
  BIGNUM* b;
  BIGNUM* c;
  BIGNUM* d;
  BIGNUM* e;
  BIGNUM* f;
  BIGNUM* g;
  BIGNUM* h;
  BIGNUM* u;
  int status;

  context = curve->context;
  BN_CTX_start(context);
  a = BN_CTX_get(context);
  b = BN_CTX_get(context);
  c = BN_CTX_get(context);
  d = BN_CTX_get(context);
  e = BN_CTX_get(context);
  f = BN_CTX_get(context);
  g = BN_CTX_get(context);
  h = BN_CTX_get(context);
  u = BN_CTX_get(context);

  status =
    (u != NULL) &&
    BN_mod_sub(a, left->y, left->x, curve->p, context) &&
    BN_mod_sub(u, right->y, right->x, curve->p, context) &&
    BN_mod_mul(a, a, u, curve->p, context) &&
    BN_mod_add(b, left->y, left->x, curve->p, context) &&
    BN_mod_add(u, right->y, right->x, curve->p, context) &&
    BN_mod_mul(b, b, u, curve->p, context) &&
    BN_mod_mul(c, left->t, curve->d2, curve->p, context) &&
    BN_mod_mul(c, c, right->t, curve->p, context) &&
    BN_mod_mul(d, left->z, right->z, curve->p, context) &&
    BN_mod_add(d, d, d, curve->p, context) &&
    BN_mod_sub(e, b, a, curve->p, context) &&
    BN_mod_sub(f, d, c, curve->p, context) &&
    BN_mod_add(g, d, c, curve->p, context) &&
    BN_mod_add(h, b, a, curve->p, context) &&
    BN_mod_mul(result->x, e, f, curve->p, context) &&
    BN_mod_mul(result->y, g, h, curve->p, context) &&
    BN_mod_mul(result->t, e, h, curve->p, context) &&
    BN_mod_mul(result->z, f, g, curve->p, context);

  BN_CTX_end(context);
  return status;
}

// result = scalar * B, double and add
static int MultiplyBase(struct Curve* curve, struct Point* result, const BIGNUM* scalar)
{
  int status;
  int bit;

  BN_zero(result->x);
  BN_zero(result->t);
  status = BN_one(result->y) && BN_one(result->z);

  for (bit = BN_num_bits(scalar) - 1; status && (bit >= 0); bit --)
    status =
      AddPoints(curve, result, result, result) &&
      (!BN_is_bit_set(scalar, bit) || AddPoints(curve, result, result, &curve->base));

  return status;
}

// y in 32 bytes little-endian, the low bit of x in the top bit
static int EncodePoint(struct Curve* curve, unsigned char* buffer, const struct Point* point)
{
  BIGNUM* inverse;
  BIGNUM* x;
  BIGNUM* y;
  int status;

  BN_CTX_start(curve->context);
  inverse = BN_CTX_get(curve->context);
  x       = BN_CTX_get(curve->context);
  y       = BN_CTX_get(curve->context);

  status =
    (y != NULL) &&
    (BN_mod_inverse(inverse, point->z, curve->p, curve->context) != NULL) &&
    BN_mod_mul(x, point->x, inverse, curve->p, curve->context) &&
    BN_mod_mul(y, point->y, inverse, curve->p, curve->context) &&
    (BN_bn2lebinpad(y, buffer, 32) == 32);

  if (status && BN_is_odd(x))
    buffer[31] |= 0x80;

  BN_CTX_end(curve->context);
  return status;
}

// SHA-512 of up to three parts, as a little-endian number modulo l
static int HashParts(struct Curve* curve, BIGNUM* result, const void* first, size_t first_size,
                     const void* second, size_t second_size, const void* third, size_t third_size)
{
  unsigned char hash[64];
  EVP_MD_CTX* context;
  int status;

  context = EVP_MD_CTX_new();
  status  =
    (context != NULL) &&
    EVP_DigestInit_ex(context, EVP_sha512(), NULL) &&
    EVP_DigestUpdate(context, first, first_size) &&
    EVP_DigestUpdate(context, second, second_size) &&
    ((third == NULL) || EVP_DigestUpdate(context, third, third_size)) &&
    EVP_DigestFinal_ex(context, hash, NULL) &&
    (BN_lebin2bn(hash, 64, result) != NULL) &&
    BN_nnmod(result, result, curve->l, curve->context);

  EVP_MD_CTX_free(context);
  return status;
}

// The signature R || S of message with the expanded key (64 bytes: the scalar before clamping, then the prefix)
static int SignExpanded(unsigned char* signature, const unsigned char* expanded, const unsigned char* message, size_t length)
{
  struct Curve curve;
  struct Point point;
  unsigned char key[32];
  unsigned char public[32];
  BIGNUM* d;
  BIGNUM* a;
  BIGNUM* r;
  BIGNUM* k;
  BIGNUM* s;
  int status;

  memcpy(key, expanded, 32);
  key[0]  &= 248;
  key[31] &= 63;
  key[31] |= 64;

  if ((curve.context = BN_CTX_new()) == NULL)
    return 0;

  BN_CTX_start(curve.context);
  curve.p      = BN_CTX_get(curve.context);
  curve.l      = BN_CTX_get(curve.context);
  curve.d2     = BN_CTX_get(curve.context);
  curve.base.x = BN_CTX_get(curve.context);
  curve.base.y = BN_CTX_get(curve.context);
  curve.base.z = BN_CTX_get(curve.context);
  curve.base.t = BN_CTX_get(curve.context);
  point.x      = BN_CTX_get(curve.context);
  point.y      = BN_CTX_get(curve.context);
  point.z      = BN_CTX_get(curve.context);
  point.t      = BN_CTX_get(curve.context);
  d            = BN_CTX_get(curve.context);
  a            = BN_CTX_get(curve.context);
  r            = BN_CTX_get(curve.context);
  k            = BN_CTX_get(curve.context);
  s            = BN_CTX_get(curve.context);

  status =
    (s != NULL) &&
    BN_hex2bn(&curve.p, ED25519_P) &&
    BN_hex2bn(&curve.l, ED25519_L) &&
    BN_hex2bn(&d, ED25519_D) &&
    BN_mod_add(curve.d2, d, d, curve.p, curve.context) &&
    BN_hex2bn(&curve.base.x, ED25519_BX) &&
    BN_hex2bn(&curve.base.y, ED25519_BY) &&
    BN_one(curve.base.z) &&
    BN_mod_mul(curve.base.t, curve.base.x, curve.base.y, curve.p, curve.context) &&
    (BN_lebin2bn(key, 32, a) != NULL) &&
    MultiplyBase(&curve, &point, a) &&
    EncodePoint(&curve, public, &point) &&
    HashParts(&curve, r, expanded + 32, 32, message, length, NULL, 0) &&
    MultiplyBase(&curve, &point, r) &&
    EncodePoint(&curve, signature, &point) &&
    HashParts(&curve, k, signature, 32, public, 32, message, length) &&
    BN_mod_mul(s, k, a, curve.l, curve.context) &&
    BN_mod_add(s, s, r, curve.l, curve.context) &&
    (BN_bn2lebinpad(s, signature + 32, 32) == 32);

  OPENSSL_cleanse(key, sizeof(key));
  BN_CTX_end(curve.context);
  BN_CTX_free(curve.context);
  return status;
}

// Support.ed25519(password, message): the signature of client_ed25519 of MariaDB, the expanded key is SHA-512 of the
// password

static int SignEd25519(lua_State* state)
{
  const char* password;
  const char* message;
  size_t size;
  size_t length;
  unsigned char expanded[64];
  unsigned char signature[64];
  int status;

  password = luaL_checklstring(state, 1, &size);
  message  = luaL_checklstring(state, 2, &length);

  status =
    EVP_Digest(password, size, expanded, NULL, EVP_sha512(), NULL) &&
    SignExpanded(signature, expanded, (const unsigned char*)message, length);

  OPENSSL_cleanse(expanded, sizeof(expanded));

  if (!status)
    return luaL_error(state, "ed25519: signing failed");

  lua_pushlstring(state, (const char*)signature, sizeof(signature));
  return 1;
}

// Support.parsec(password, salt, factor, message): the signature of parsec of MariaDB, the key is PBKDF2-HMAC-SHA512
// of the password with the salt over 1024 << factor rounds, the signature Ed25519 of EVP with that key as the seed

static int SignParsec(lua_State* state)
{
  const char* password;
  const char* salt;
  const char* message;
  size_t size;
  size_t length;
  size_t count;
  lua_Integer factor;
  unsigned char key[32];
  unsigned char signature[64];
  EVP_PKEY* pair;
  EVP_MD_CTX* context;
  int status;

  password = luaL_checklstring(state, 1, &size);
  salt     = luaL_checklstring(state, 2, &length);
  factor   = luaL_checkinteger(state, 3);
  message  = luaL_checklstring(state, 4, &count);

  if ((factor < 0) || (factor > 20))
    return luaL_error(state, "parsec: the iteration factor %d is out of range", (int)factor);

  pair    = NULL;
  context = NULL;
  status  =
    PKCS5_PBKDF2_HMAC(password, (int)size, (const unsigned char*)salt, (int)length, 1024 << factor, EVP_sha512(),
                      sizeof(key), key) &&
    ((pair = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, NULL, key, sizeof(key))) != NULL) &&
    ((context = EVP_MD_CTX_new()) != NULL) &&
    (EVP_DigestSignInit(context, NULL, NULL, NULL, pair) == 1) &&
    ((length = sizeof(signature)) > 0) &&
    (EVP_DigestSign(context, signature, &length, (const unsigned char*)message, count) == 1);

  OPENSSL_cleanse(key, sizeof(key));
  EVP_MD_CTX_free(context);
  EVP_PKEY_free(pair);

  if (!status)
    return luaL_error(state, "parsec: signing failed");

  lua_pushlstring(state, (const char*)signature, sizeof(signature));
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
    { "connect",    ConnectSession    },
    { "encrypt",    EncryptData       },
    { "uncompress", UncompressData    },
    { "crc32",      CalculateChecksum },
    { "ed25519",    SignEd25519       },
    { "parsec",     SignParsec        },
    { NULL,         NULL              }
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
