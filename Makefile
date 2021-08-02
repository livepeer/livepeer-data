version ?= $(shell git describe --tag --dirty)
cmd ?= analyzer

allCmds := $(shell ls ./cmd/)
dockerimg := livepeer/data

.PHONY: all $(allCmds) docker docker_run docker_push

all: $(allCmds)

$(allCmds):
	$(MAKE) -C ./cmd/$@

run:
	$(MAKE) -C ./cmd/$(cmd) run

docker:
	docker build -t $(dockerimg) -t $(dockerimg):$(version) --build-arg version=$(version) .

docker_run: docker
	docker run -it --rm --network=host --entrypoint=./$(cmd) $(dockerimg) $(args)

docker_push:
	docker push $(dockerimg):$(version)
	docker push $(dockerimg):latest
