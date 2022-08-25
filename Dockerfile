FROM	golang:1.16-alpine	as	builder

RUN	apk add --no-cache --update make

WORKDIR	/app

ENV	GOFLAGS	"-mod=readonly"

COPY	go.mod	go.sum	./

RUN	go mod download

ARG	version

RUN	echo $version

COPY	.	.

RUN	make "version=$version"

FROM	alpine:latest

WORKDIR	/app

COPY --from=builder	/app/build/*	/usr/local/bin/
