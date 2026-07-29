\
MODULE := minipulsar

all: generate build

generate:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	PATH="$$PATH:$$(go env GOPATH)/bin" protoc --go_out=. --go_opt=paths=source_relative pb/PulsarApi.proto

build:
	mkdir -p bin
	go build -o bin/minipulsar ./cmd/minipulsar

test:
	go test ./...

clean:
	rm -f bin/*
	rm -f pb/PulsarApi.pb.go
