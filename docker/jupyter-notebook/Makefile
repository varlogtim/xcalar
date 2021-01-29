.PHONY: all run build clean

DC_VERSION=1.16.1
DC=docker-compose-$(DC_VERSION)

all: run

run: $(DC) build
	./$(DC) up -d
	sleep 8
	./$(DC) logs --tail=100
	echo "Use the address above, replacing http://localhost with http://`hostname`"

build: $(DC)
	./$(DC) build

clean: $(DC)
	-./$(DC) down
	-./$(DC) rm

$(DC):
	curl -sL https://github.com/docker/compose/releases/download/$(DC_VERSION)/docker-compose-`uname -s`-`uname -m` > $@
	chmod +x $@

