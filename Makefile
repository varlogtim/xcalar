.PHONY: all full docker build qa xce sanity package venv clean config gui setup mysql sqldf statsDoc viewStatsDo caddy
SHELL = /bin/bash

TOP        = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
OSID       = $(shell osid)
XCE_HOME   = /var/opt/xcalar
DESTDIR    = /tmp/$(shell id -un)-$(shell id -u)/rootfs
VENV       = xcve
ACTIVATE   = . $(VENV)/bin/activate
BUILD_DIR  = $(XLRDIR)/buildOut
HOST      ?= $(shell hostname -f)
HOSTIP    ?= $(shell hostname -i)
BUILD_TYPE ?= prod
COVERAGE   ?= OFF

DATADIR          = $(XLRDIR)/src/data
QATEST_DIR = $(DATADIR)
XCE_LICENSE      = $(DATADIR)/XcalarLic.key
XCE_LICENSEDIR   = $(DATADIR)
XCE_QALICENSEDIR = $(XCE_LICENSEDIR)
XCE_HTTPS_PORT  = 8443

CADDYHOME  = /tmp/caddy-$(shell id -u)/$(XCE_HTTPS_PORT)

SQLDF_PATH = src/sqldf/sbt/target/xcalar-sqldf.jar

DOCKER_SOCKET ?= /var/run/docker.sock
DOCKER := $(shell test -w $(DOCKER_SOCKET) && echo docker || echo sudo docker)

include src/3rd/spark/BUILD_ENV

export SCALA_VERSION SQLDF_VERSION SPARK_VERSION

export BUILD_DIR BUILD_TYPE DESTDIR VENV

export XCE_HTTPS_PORT

_all:
	make -s -C docker V=0
	XTRACE=1 crunpwd el7-build $(MAKE) all

all: clean venv config qa gui docker setup cluster

build: venv config qa

venv: $(VENV)/SHASUMS

$(VENV)/SHASUMS: requirements.txt pkg/requirements.txt
	bin/pyVeSetup.sh "$(@D)" -r pkg/requirements.txt -r requirements.txt

docker:
	$(MAKE) -C docker

package: venv docker
	. bin/xcsetenv && ./bin/build-installers.sh

agent: docker
	cd docker/el7/el7-build-agent && make

$(BUILD_DIR)/rules.ninja: CMakeLists.txt
	mkdir -p $(BUILD_DIR)
	. bin/xcsetenv && cmBuild config $(BUILD_TYPE)

gui: $(XLRGUIDIR)/updated

$(XLRGUIDIR)/updated:
	. bin/xcsetenv && $(MAKE) -C $(XLRGUIDIR) dev
	touch $@

cluster:
	. bin/xcsetenv && xc2 cluster start

cleanbins:
	cd $(XLRDIR)/bin && rm -fv childnode licenseCheck signKey readKey usrnode xccli xcmgmtd xcmonitor xcUpgradeTool dbgen xcMapRClient jsPackage

clean: xclean cleanbins
	. bin/xcsetenv && cmBuild clean

xclean:
	@. doc/env/xc_aliases && xclean

clean-gui:
	rm -f $(XLRGUIDIR)/updated

config:
	@echo $@
	. bin/xcsetenv && cmBuild config $(BUILD_TYPE) -DCOVERAGE=$(COVERAGE)

xce:
	@echo $@
	. bin/xcsetenv && cmBuild xce

installedObjects:
	@echo $@
	. bin/xcsetenv && cmBuild installedObjects

qa:
	@echo $@
	. bin/xcsetenv && cmBuild qa

sqldf:
	@mkdir -p $(dir $(SQLDF_PATH))
	@bin/download-sqldf.sh $(SQLDF_PATH)
	@echo sqldf.jar downloaded to $(SQLDF_PATH)

caddy: $(CADDYHOME)/caddy.pid

killcaddy:
	 test -e $(CADDYHOME)/caddy.pid && pkill -e -F $(CADDYHOME)/caddy.pid || true

$(CADDYHOME)/caddy.pid:
	@mkdir -p $(@D)
	@bin/caddy.sh start -p $(XCE_HTTPS_PORT) > $(CADDYHOME)/env 2>/dev/null || bin/caddy.sh start -p $(XCE_HTTPS_PORT)
	@echo
	@if [ -n "$${container}" ]; then echo "** Connect to XD: https://$(HOST):$$($(DOCKER) port `hostname` $(XCE_HTTPS_PORT)/tcp | cut -d: -f2 || echo "$(XCE_HTTPS_PORT)") **"; else echo "Connect to https://`hostname -f`:$(XCE_HTTPS_PORT) or https://localhost:$(XCE_HTTPS_PORT) when on `hostname -f`"; fi
	@echo

mysql:
	@test -e /usr/lib/systemd/system/mariadb.service && sudo systemctl start mariadb.service || true
	@test -e /lib/systemd/system/mysql.service && sudo systemctl start mysql.service || true

setup: xclean sqldf caddy
	@sudo mkdir -p /var/www
	@sudo ln -sfn $(XLRGUIDIR)/xcalar-gui /var/www/
	@rm -rf /var/tmp/xcalar-`id -un`/*
	@mkdir -p /var/tmp/xcalar-`id -un`/sessions
	@mkdir -p $(XCE_HOME)/jupyterNotebooks
	@ln -sfn $(XLRGUIDIR)/xcalar-gui/assets/jupyter/jupyter "$(XCE_HOME)"/.jupyter
	@ln -sfn $(XLRGUIDIR)/xcalar-gui/assets/jupyter/ipython "$(XCE_HOME)"/.ipython
	@ln -sfn $(XLRGUIDIR)/xcalar-gui/assets/jupyter/jupyter ~/.jupyter
	@ln -sfn $(XLRGUIDIR)/xcalar-gui/assets/jupyter/ipython ~/.ipython
	@mkdir -p -m 0700 $(XCE_HOME)/config && cp $(XLRDIR)/pkg/docker/defaultAdmin.json $(XCE_HOME)/config/ && chmod 0600 $(XCE_HOME)/config/defaultAdmin.json

#sanity: clean venv gui config xce setup
sanity: clean venv config setup
	@echo $@
	. bin/xcsetenv && cmBuild sanity

pytest.%: xclean caddy
	. bin/xcsetenv && /bin/bash -x src/bin/tests/pyTestNew/PyTest.sh -k test_$*

statsDoc:
	@echo "Building Stats documentation..."
	@cd doc/stats && python generate.py
	@echo "Done. View stats at $(XLRDIR)/doc/stats/stats.html"

viewStatsDoc: statsDoc
	google-chrome $(XLRDIR)/doc/stats/stats.html
