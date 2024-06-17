fmt:
	go fmt ./...

vet:
	go vet ./...

.PHONY: build
build: fmt vet
	go build -v -o bin/kitedb ./server

test: build
	go test -cover -race ./...

test_coverage:
	go test -coverprofile=coverage.out ./...; \
    go tool cover -html="coverage.out"

lint:
	golangci-lint run

clean:
	go clean
	rm -rf bin/