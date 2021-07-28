version ?= $(shell git describe --dirty)
ldflags := -X 'main.Version=$(version)'

builddir := ./build/
dockerimg := livepeer/data

all: healthlyzer

.PHONY: all healthlyzer run docker docker_run docker_push

healthlyzer:
	go build -o $(builddir) -ldflags="$(ldflags)" cmd/healthlyzer/healthlyzer.go

run:
	go run -ldflags="$(ldflags)" cmd/healthlyzer/healthlyzer.go $(args)

docker:
	docker build -t $(dockerimg) -t $(dockerimg):$(version) --build-arg version=$(version) .

docker_run: docker
	docker run -it --rm --network=host $(dockerimg) $(args)

docker_push:
	docker push $(dockerimg):$(version)
	docker push $(dockerimg):latest
