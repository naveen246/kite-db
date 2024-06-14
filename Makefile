.PHONY: build
build:
	go build -v -o bin/kite ./cmd

test: build
	go test -cover -race ./...

lint:
	#brew install golangci-lint
	golangci-lint run

clean:
	rm -rf bin/