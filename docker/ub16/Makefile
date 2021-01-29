.PHONY: all ub16-build

all: ub16-build

#ub16-build/.docker: ub16-build/Dockerfile
#	docker build -t $(@D) -f $< \
#		--build-arg=CONTAINER_USER=$(CONTAINER_USER) \
#		--build-arg=CONTAINER_UID=$(CONTAINER_UID) \
#		--build-arg=http_proxy=$(http_proxy) $(XLRDIR)

ub16-build: ub16-build/Dockerfile $(XLRDIR)/pkg/node/package.json
	tar cf - -C $(XLRDIR) docker/ub16/ub16-build/Dockerfile $$(grep -E '(ADD|COPY)' ub16-build/Dockerfile | awk '{print $$2}' | sort | uniq) | \
    docker build -t $@ -f docker/ub16/$< \
       --build-arg=CONTAINER_USER=$(CONTAINER_USER) \
       --build-arg=CONTAINER_UID=$(CONTAINER_UID) \
       --build-arg=http_proxy=$(http_proxy) -


include ../common.mk
-include ../local.mk
