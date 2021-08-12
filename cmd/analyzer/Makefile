version ?= $(shell git describe --tag --dirty)

ldflags := -X 'main.Version=$(version)'
builddir := ../../build/

.PHONY: all build run

all: build

build:
	go build -o $(builddir) -ldflags="$(ldflags)" analyzer.go

run:
	go run -ldflags="$(ldflags)" analyzer.go $(args)
