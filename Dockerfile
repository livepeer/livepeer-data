FROM	golang:1.20.2-alpine	as	builder

RUN	apk add --no-cache --update make

WORKDIR	/app

ENV	GOFLAGS	"-mod=readonly"

COPY	go.mod	go.sum	./

RUN	go mod download

ARG	version

RUN	echo $version

COPY	.	.

RUN	make "version=$version"

FROM	alpine:3.16

RUN	apk add --no-cache ca-certificates

WORKDIR	/app

COPY --from=builder	/app/build/*	/usr/local/bin/
