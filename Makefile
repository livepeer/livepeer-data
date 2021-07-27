version ?= $(shell git describe --dirty)
ldflags := -X 'main.Version=$(version)'

all: healthystreams

.PHONY: all healthystreams run docker push

healthystreams:
	go build -ldflags="$(ldflags)" -o healthystreams main.go

run:
	go run -ldflags="$(ldflags)" main.go $(args)

docker:
	docker build -t livepeer/healthystreams:latest --build-arg version=$(version) .

docker_run: docker
	docker run -it --rm --network=host livepeer/healthystreams $(args)

push:
	docker push livepeer/healthystreams:latest
