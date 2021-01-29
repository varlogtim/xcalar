.PHONY: all el7 amzn1 gui-installer docker rpm installer build-installer

# XXX DO NOT MERGE- We must figure out a way to bring this in
GIT_SHA1 = $(shell git rev-parse --verify HEAD | cut -c1-8)
CURRENT_DIR = $(shell pwd)

all: installer

amzn1: docker
	./bin/build-offline.sh amzn1
	./bin/installer/mkInstaller.sh amzn1

amzn2: docker
	./bin/build-offline.sh amzn2
	./bin/installer/mkInstaller.sh amzn2

el7: docker
	./bin/build-offline.sh  el7
	./bin/installer/mkInstaller.sh el7
	./bin/build-user-installer.sh el7
	./bin/build-gui-installer.sh el7

gui-installer: all
	$(MAKE) -C ./pkg/gui-installer

rpm:
	./bin/build-offline.sh amzn2 el7

installer: docker rpm
	./bin/installer/mkInstaller.sh
	./bin/build-user-installer.sh el7
	./bin/build-gui-installer.sh el7

docker:
	$(MAKE) -C docker build-installer
