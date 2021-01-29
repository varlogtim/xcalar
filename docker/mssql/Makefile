.PHONY: all sqlcmd clean

SHELL=/bin/bash

NAME=mssql
MSSQL_HOST=mssql
MSSQL_USER=sa
IMAGE=microsoft/mssql-server-linux
ENVFILE=.env
ARGS=
NETSTORE = $(shell ls /netstore >/dev/null && readlink -f /netstore || echo /netstore)
VOLUME = mssql-data
NETWORK = mssqlnet

all: $(ENVFILE)
	@docker pull $(IMAGE)
	@docker network inspect $(NETWORK) >/dev/null 2>&1 || docker network create $(NETWORK)
	@docker volume inspect $(VOLUME) >/dev/null 2>&1 || docker volume create --name $(VOLUME)
	@eval `cat $(ENVFILE)`; docker inspect $(NAME) >/dev/null 2>&1 || { \
		docker run -d -v $(VOLUME):/var/opt/mssql --network $(NETWORK) --restart unless-stopped -v $(NETSTORE):/netstore --name $(NAME) -p 1433:1433 --hostname "$${MSSQL_HOST}" -it --env-file $(ENVFILE) -e ACCEPT_EULA=Y $(IMAGE) && \
		sleep 3; }

$(ENVFILE):
	@echo "Installing default environment with passwords into $@"
	@cp default.env $@

shell: all
	@eval `cat $(ENVFILE)` && docker exec -it $(NAME) bash -l

sqlcmd:
	@eval `cat $(ENVFILE)` && docker exec -it $(NAME) /opt/mssql-tools/bin/sqlcmd \
		-S "$${MSSQL_HOST}" -U "$${MSSQL_USER}" -P "$${SA_PASSWORD}" $(ARGS)

sqlcmd_run: $(ENVFILE)
	@eval `cat $(ENVFILE)` && docker run -it --network $(NETWORK) --rm $(IMAGE) /opt/mssql-tools/bin/sqlcmd -S "$${MSSQL_HOST}" -U "$${MSSQL_USER}" -P "$${SA_PASSWORD}" $(ARGS)

bcp:
	@eval `cat $(ENVFILE)` && docker exec -it $(NAME) /opt/mssql-tools/bin/bcp -S "$${MSSQL_HOST}" -U "$${MSSQL_USER}" -P "$${SA_PASSWORD}" $(ARGS)

clean:
	@docker inspect $(NAME) >/dev/null 2>&1 && docker rm -f $(NAME) >/dev/null || true
