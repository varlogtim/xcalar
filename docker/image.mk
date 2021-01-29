image_mk_dir := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

TAG ?= v5
TAG_NEXT ?= v6
REGISTRY ?= registry.int.xcalar.com

#CENTOS6=centos@sha256:24d17fa6c174019858b4a90df44443e8fad3cfa0f04c8ef31e4a9e80d776df85
#CENTOS7=centos@sha256:4a701376d03f6b39b8c2a8f4a8e499441b0d567f9ab9d58e4991de4472fb813c
#
#AMZN1=amazonlinux@sha256:5ded99e84532e28bbcf5ad466aeddc54eca2622a21e6f393cc6469c6ca8b1d2b
#AMZN2=amazonlinux@sha256:5aa0460abffafc6a76590f0070e1b243a93b7bbe7c8035f98c1dee2f9b46f44c
#
#UBI7=registry.access.redhat.com/ubi7/ubi@sha256:3e28d6b64932eda2904238af77d8e10186087cc5da36e1cc951db6953d2d815b
#UBI7_INIT=registry.access.redhat.com/ubi7/ubi-init@sha256:6fef8892fce4791c441069ac02d9a136416e078c6525f42b86bbe9d2d2ad2a29
#
## trusty-20190305
#UB14=ubuntu@sha256:6612de24437f6f01d6a2988ed9a36b3603df06e8d2c0493678f3ee696bc4bb2d
#UB16=ubuntu@sha256:58d0da8bc2f434983c6ca4713b08be00ff5586eb5cdff47bcde4b2e88fd40f88
#UB18=ubuntu@sha256:017eef0b616011647b269b5c65826e2e2ebddbe5d1f8c1e56b3599fb14fabec8

FPM=$(REGISTRY)/fpm:latest

EL6_BASE=$(REGISTRY)/xcalar/el6-base:v2019-08-23
EL7_BASE=$(REGISTRY)/xcalar/el7-base:$(TAG)
EL7_BUILD=$(REGISTRY)/xcalar/el7-build:$(TAG)

# Images used by the build installer process
# names are lowercase as they are used in scripts and as tags for docker
# vars need to be exported to be available to shell outside of the makefile
el7_build=$(REGISTRY)/xcalar/el7-build:$(TAG)
el7_offline=$(REGISTRY)/xcalar/el7-offline:$(TAG)
el7_build_agent=$(REGISTRY)/xcalar/el7-build-agent:$(TAG)
el7_vsts_agent=$(REGISTRY)/xcalar/el7-vsts-agent:$(TAG)
amzn1_build=$(REGISTRY)/xcalar/amzn1-build:$(TAG)
amzn1_offline=$(REGISTRY)/xcalar/amzn1-offline:$(TAG)
amzn2_build=$(REGISTRY)/xcalar/amzn2-build:$(TAG)
amzn2_offline=$(REGISTRY)/xcalar/amzn2-offline:$(TAG)
ub18_systemd=$(REGISTRY)/xcalar/ub18-systemd:$(TAG)
ub14_build=$(REGISTRY)/xcalar/ub14-build:$(TAG)
ubi7_build=$(REGISTRY)/xcalar/ubi7-build:$(TAG)

include $(image_mk_dir)/image.env
# vim: ft=make
