PREFIX    ?= /usr
LUADIR    ?= $(PREFIX)/share/tarantool
CC        ?= cc
CFLAGS    ?= -O2 -g
TARANTOOL ?= tarantool
SUPPORT   ?= 1
HOST      ?=
SSH       ?= ssh

# MirrorFlayer.Support (Source/Support.so) is a C module: TLS over the connected socket, RSA for caching_sha2_password
# and inflate for the compressed protocol and binlog events. It needs the headers of tarantool-dev, libssl-dev and
# zlib1g-dev; SUPPORT=0 leaves it out, then only plain uncompressed connections and mysql_native_password (or
# caching_sha2_password from its cache) work
# Support.so is stripped with <triplet>-strip of $(CC) when there is one (a cross build), with strip otherwise;
# STRIP=true keeps the debug info
# install puts the Lua modules into $(LUADIR)/MirrorFlayer and Support.so into $(LIBDIR)/MirrorFlayer: LUADIR and LIBDIR
# give another tree, such as Modules and Modules/<arch> of an application

MACHINE := $(shell $(CC) -dumpmachine)
LIBDIR  ?= $(PREFIX)/lib/$(MACHINE)/tarantool
STRIP   ?= $(if $(shell command -v $(MACHINE)-strip 2>/dev/null),$(MACHINE)-strip,strip)
MODULES := $(wildcard Source/*.lua)
MYSQL   := $(wildcard Source/MySQL/*.lua)
PGSQL   := $(wildcard Source/PostgreSQL/*.lua)
LIBRARY := $(if $(filter-out 0,$(SUPPORT)),Source/Support.so)

override CFLAGS += -std=gnu99 -Wall -fPIC $(shell pkg-config --cflags tarantool 2>/dev/null || echo -I/usr/include/tarantool)

all: $(LIBRARY)

Source/Support.so: Source/Support.c
	$(CC) $(CFLAGS) -shared -z noexecstack $< $(LDFLAGS) -lssl -lcrypto -lz -o $@
	$(STRIP) $@

check:
	@for file in $(MODULES) $(MYSQL) $(PGSQL) Tests/*.lua; do \
	  $(TARANTOOL) -e "assert(loadfile('$$file')) os.exit(0)" || exit 1; \
	done
	@echo checked

install: all
	install -d $(DESTDIR)$(LUADIR)/MirrorFlayer/MySQL $(DESTDIR)$(LUADIR)/MirrorFlayer/PostgreSQL
	install -m 644 $(MODULES) $(DESTDIR)$(LUADIR)/MirrorFlayer/
	install -m 644 $(MYSQL) $(DESTDIR)$(LUADIR)/MirrorFlayer/MySQL/
	install -m 644 $(PGSQL) $(DESTDIR)$(LUADIR)/MirrorFlayer/PostgreSQL/
ifneq ($(LIBRARY),)
	install -d $(DESTDIR)$(LIBDIR)/MirrorFlayer
	install -m 644 $(LIBRARY) $(DESTDIR)$(LIBDIR)/MirrorFlayer/
endif

uninstall:
	rm -rf $(DESTDIR)$(LUADIR)/MirrorFlayer $(DESTDIR)$(LIBDIR)/MirrorFlayer

clean:
	rm -f Source/Support.so

# Docker test on this machine
test:
	Tests/DockerTest.sh

# Docker test on a remote Docker host
remote-test:
	@test -n "$(HOST)" || { echo "usage: make remote-test HOST=docker-host"; exit 1; }
	SSH="$(SSH)" Tests/RemoteTest.sh $(HOST)

.PHONY: all check install uninstall clean test remote-test
