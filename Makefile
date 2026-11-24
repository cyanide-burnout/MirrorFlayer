PREFIX    ?= /usr
LUADIR    ?= $(PREFIX)/share/tarantool
CC        ?= cc
CFLAGS    ?= -O2 -g
TARANTOOL ?= tarantool
TLS       ?= 1
HOST      ?=
SSH       ?= ssh

# MirrorFlayer.TLS (Source/TLS.so) is a C module: TLS over the connected socket and RSA for caching_sha2_password.
# It needs the headers of tarantool-dev and libssl-dev; TLS=0 leaves it out, then only plain connections and
# mysql_native_password (or caching_sha2_password from its cache) work

MACHINE := $(shell $(CC) -dumpmachine)
LIBDIR  ?= $(PREFIX)/lib/$(MACHINE)/tarantool
MODULES := $(wildcard Source/*.lua)
LIBRARY := $(if $(filter-out 0,$(TLS)),Source/TLS.so)

override CFLAGS += -std=gnu99 -Wall -fPIC $(shell pkg-config --cflags tarantool 2>/dev/null || echo -I/usr/include/tarantool)

all: $(LIBRARY)

Source/TLS.so: Source/TLS.c
	$(CC) $(CFLAGS) -shared $< $(LDFLAGS) -lssl -lcrypto -o $@

check:
	@for file in $(MODULES) Tests/*.lua; do \
	  $(TARANTOOL) -e "assert(loadfile('$$file')) os.exit(0)" || exit 1; \
	done
	@echo checked

install: all
	install -d $(DESTDIR)$(LUADIR)/MirrorFlayer
	install -m 644 $(MODULES) $(DESTDIR)$(LUADIR)/MirrorFlayer/
ifneq ($(LIBRARY),)
	install -d $(DESTDIR)$(LIBDIR)/MirrorFlayer
	install -m 644 $(LIBRARY) $(DESTDIR)$(LIBDIR)/MirrorFlayer/
endif

uninstall:
	rm -rf $(DESTDIR)$(LUADIR)/MirrorFlayer $(DESTDIR)$(LIBDIR)/MirrorFlayer

clean:
	rm -f Source/TLS.so

# Docker test on this machine
test:
	Tests/DockerTest.sh

# Docker test on a remote Docker host
remote-test:
	SSH="$(SSH)" Tests/RemoteTest.sh $(HOST)

.PHONY: all check install uninstall clean test remote-test
