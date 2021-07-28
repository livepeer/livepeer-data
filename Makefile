version ?= $(shell git describe --dirty)
ldflags := -X 'main.Version=$(version)'
builddir := ./build/

all: healthlyzer

.PHONY: all healthlyzer run docker push

healthlyzer:
	go build -o $(builddir) -ldflags="$(ldflags)" cmd/healthlyzer/healthlyzer.go

run:
	go run -ldflags="$(ldflags)" cmd/healthlyzer/healthlyzer.go $(args)

docker:
	docker build -t livepeer/lpdata:latest --build-arg version=$(version) .

docker_run: docker
	docker run -it --rm --network=host livepeer/lpdata $(args)

docker_push:
	docker push livepeer/lpdata:latest
