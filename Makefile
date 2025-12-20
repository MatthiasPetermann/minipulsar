\
MODULE := minipulsar

all: generate build

generate:
	protoc --go_out=. --go_opt=paths=source_relative pb/PulsarApi.proto

build:
	mkdir -p bin
	go build -o bin/minipulsar

test:
	go test ./...

clean:
	rm -f bin/*
	rm -f pb/PulsarApi.pb.go
