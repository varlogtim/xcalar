.PHONY: network clean default minimal up down

SHELL=/bin/bash


include ../common.mk

default: up
minimal: up

up: network

network:
	@$(DOCKER) network inspect $(NETWORK) >/dev/null 2>&1 || \
		$(DOCKER) network create \
            --subnet $(SUBNET) \
            --gateway $(GATEWAY) \
            -d bridge \
            --opt com.docker.network.bridge.enable_icc=true \
            --opt com.docker.network.bridge.enable_ip_masquerade=true \
            --opt com.docker.network.bridge.host_binding_ipv4=0.0.0.0 \
            --opt com.docker.network.bridge.name=$(NETWORK)0 \
            --opt com.docker.network.driver.mtu=1500 \
            --label owner=dcc \
            --label cluster="xcalar" \
            $(NETWORK)

down: clean

clean:
	-$(DOCKER) network rm $(NETWORK) || true
	-ip link delete $(NETWORK)0 type bridge || true
