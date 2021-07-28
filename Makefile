version ?= $(shell git describe --dirty)
ldflags := -X 'main.Version=$(version)'

builddir := ./build/
dockerimg := livepeer/data

all: healthanalyzer

.PHONY: all healthanalyzer run docker docker_run docker_push

healthanalyzer:
	go build -o $(builddir) -ldflags="$(ldflags)" cmd/healthanalyzer/healthanalyzer.go

run:
	go run -ldflags="$(ldflags)" cmd/healthanalyzer/healthanalyzer.go $(args)

docker:
	docker build -t $(dockerimg) -t $(dockerimg):$(version) --build-arg version=$(version) .

docker_run: docker
	docker run -it --rm --network=host $(dockerimg) $(args)

docker_push:
	docker push $(dockerimg):$(version)
	docker push $(dockerimg):latest
