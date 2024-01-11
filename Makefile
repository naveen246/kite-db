
.PHONY: build
build:
	go build -v -o bin/kite ./cmd

test: build
	go test -cover -race ./...

lint:
	#brew install golangci-lint
	golangci-lint run

.PHONY: proto
proto:
	cd proto && \
	protoc \
		--go_out=. \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="${GOPATH}/bin/protoc-gen-go" \
    	--go-grpc_out=. \
    	--go-grpc_opt paths=source_relative \
    	--plugin protoc-gen-go-grpc="${GOPATH}/bin/protoc-gen-go-grpc" \
      	--go-vtproto_out=. \
      	--go-vtproto_opt paths=source_relative \
      	--plugin protoc-gen-go-vtproto="${GOPATH}/bin/protoc-gen-go-vtproto" \
  	 	--go-vtproto_opt=features=marshal+unmarshal+size+pool+equal+clone \
	    *.proto

proto_clean:
	rm -f */*.pb.go

proto_format:
	#brew install clang-format
	clang-format -i --style=Google proto/*.proto

proto_doc:
	#go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc
	protoc --doc_out=docs/proto --doc_opt=markdown,proto.md proto/*.proto
