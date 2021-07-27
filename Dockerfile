FROM golang:1.16-alpine as builder

RUN apk add --update make

WORKDIR /build

ENV GOFLAGS "-mod=readonly"

COPY go.mod go.sum ./

RUN go mod download

ARG version
RUN echo $version

COPY . .

RUN make healthystreams "version=$version"

FROM alpine

WORKDIR /app

COPY --from=builder /build/healthystreams .

ENTRYPOINT [ "./healthystreams" ]
