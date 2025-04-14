# Makefile

# This Makefile is used to build and run the distributed task queue application.

.PHONY: all build run clean

all: build

build:
	go build -o taskqueueservice ./cmd/taskqueueservice

run: build
	./taskqueueservice

clean:
	rm -f taskqueueservice