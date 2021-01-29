.PHONY: all pyarrow clean

mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(patsubst %/,%,$(dir $(mkfile_path)))

all: pyarrow

pyarrow: .docker-base
	docker run -e PYTHON_VERSIONS="3.6,16" --shm-size=2g --rm -t -i -v $(current_dir):/io arrow_manylinux1_x86_64_base /io/build_arrow.sh
	#docker run --rm -t -v $(current_dir):/io arrow-base-x86_64 chown -R `stat -c %u:%g $(current_dir)` /io/dist

clean:
	rm -rf dist/*

.docker-base: Dockerfile-x86_64_base scripts/*.sh
	docker build -t arrow_manylinux1_x86_64_base -f Dockerfile-x86_64_base .
