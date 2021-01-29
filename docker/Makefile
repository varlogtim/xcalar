# Full directory path to this Makefile, eg: /home/abakshi/xcalar/docker
mkfile_dir  := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

SHELL = /bin/bash

SUBDIRS = el7 amzn2 ub18

.PHONY: default minimal all $(SUBDIRS) deps update pull pull-installer pull-dev push upload xcalarnet

IMAGES_TO_PUSH = el7-base el7-systemd el7-build el7-offline el7-build-agent amzn2-build \
				 amzn2-offline

IMAGES_NICE_TO_HAVE = amzn1-build ub14-build

UNAME := $(shell uname)

ifeq ($(UNAME),Linux)
	SHASUM=sha256sum
else
	SHASUM=shasum -a 256
endif

include $(mkfile_dir)/common.mk

default: minimal

all: alldirs xcalarnet

alldirs: $(SUBDIRS)
%: alldirs

minimal: xcalarnet
	@$(MAKE) V=$(V) $(QUIET) -C el7 el7-build/.docker

xcalarnet:
	@echo Building $@ ...
	@cd $@ && $(MAKE) V=$(V) $(QUIET)

$(SUBDIRS):
	@echo Building all $@ ...
	@$(MAKE) V=$(V) $(QUIET) $(filter-out $@,$(MAKECMDGOALS)) -C $@

# pull all the -build and -offline images to build the installer
# if locally built images already exist the tag will shift the pointer
# to the downloaded one
pull-installer:
	for buildDep in el7-build el7-offline amzn1-offline amzn2-offline; do \
		image=$(REGISTRY)/xcalar/$${buildDep}:$(TAG); \
		$(DOCKER) pull $${image} && $(DOCKER) tag $${image} $${buildDep}; \
	done

pull-dev:
	$(DOCKER) pull $(REGISTRY)/xcalar/el7-build:$(TAG) && $(DOCKER) tag $(REGISTRY)/xcalar/el7-build:$(TAG) el7-build

build-installer:
	$(MAKE) V=$(V) $(QUIET) -C el7 el7-build/.docker el7-offline/.docker
	$(MAKE) V=$(V) $(QUIET) -C amzn2 amzn2-offline/.docker

deps:
	set -e && for distro in el7; do (cd $$distro && rm -f $$distro-deps.tar && make V=$(V) $(QUIET) distro-deps.tar; done

define Fpush_image

.PHONY: untag-$(1)
untag-$(1):
	-$(DOCKER) rmi $(REGISTRY)/xcalar/$(1):$(TAG)

.PHONY: tag-$(1)
tag-$(1):
	$(DOCKER) tag $(1) $(REGISTRY)/xcalar/$(1):$(TAG)

.PHONY: push-$(1)
push-$(1): tag-$(1)
	@echo "Pushing $(REGISTRY)/xcalar/$(1):$(TAG)"
	$(DOCKER) push $(REGISTRY)/xcalar/$(1):$(TAG)
	@digest=$$$$($(DOCKER) inspect $(REGISTRY)/xcalar/$(1):$(TAG) --format='{{index .RepoDigests 0}}') && \
       sed -i 's,$(REGISTRY)/xcalar/$(1)[:@].*$$$$,$(REGISTRY)/xcalar/$(1):$(TAG)    # '$$$${digest}',' image.env

endef


$(foreach image,$(IMAGES_TO_PUSH),$(eval $(call Fpush_image,$(image))))

tagall: $(patsubst %,tag-%,$(IMAGES_TO_PUSH))
untagall: $(patsubst %,untag-%,$(IMAGES_TO_PUSH))
pushall: $(patsubst %,push-%,$(IMAGES_TO_PUSH))

push:
	make OFFICIAL=1 V=$(V) $(QUIET) all
	make OFFICIAL=1 V=$(V) $(QUIET) $(patsubst %,push-%,$(IMAGES_TO_PUSH))
