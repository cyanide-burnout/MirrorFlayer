PREFIX    ?= /usr
LUADIR    ?= $(PREFIX)/share/tarantool
TARANTOOL ?= tarantool
TLS       ?= 0
HOST      ?=
SSH       ?= ssh

MODULES := $(wildcard Source/*.lua)

ifneq ($(TLS),0)
$(error TLS=1: mirrorflayer.tls is not implemented yet, build with TLS=0)
endif

all:

check:
	@for file in $(MODULES) Tests/*.lua; do \
	  $(TARANTOOL) -e "assert(loadfile('$$file')) os.exit(0)" || exit 1; \
	done
	@echo checked

install:
	install -d $(DESTDIR)$(LUADIR)/mirrorflayer
	install -m 644 $(MODULES) $(DESTDIR)$(LUADIR)/mirrorflayer/

uninstall:
	rm -rf $(DESTDIR)$(LUADIR)/mirrorflayer

# Docker test on this machine
test:
	Tests/DockerTest.sh

# Docker test on a remote Docker host
remote-test:
	SSH="$(SSH)" Tests/RemoteTest.sh $(HOST)

.PHONY: all check install uninstall test remote-test
