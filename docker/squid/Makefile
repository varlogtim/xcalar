.PHONY: all squid download shell stop clean

-include local.mk

CACHE_DIR = /var/spool/squid
NAME ?= cacher

NAME ?= squid
VOLUME ?= squid

all: squid
	docker rm -f $(NAME) || true
	docker run -d --init --name $(NAME) --hostname $(NAME) --restart=unless-stopped --network host -p 3128:3128 -v $(VOLUME):/var/spool/squid squid

stop:
	docker stop $(NAME)

clean: stop
	docker rm -f $(NAME) || true
	rm -f storeid_rpmdeb

storeid_rpmdeb: storeid_rpmdeb.go
	if command -v go >/dev/null; then go build -o $@ $@.go; else http_proxy= https_proxy= curl -fsSL http://repo.xcalar.net/deps/storeid_rpmdeb-2 -o $@; fi
	chmod +x $@

shell:
	docker exec -it $(NAME) bash -l

download:
	chmod +x storeid_rpmdeb

squid: storeid_rpmdeb
	docker build -t squid .
