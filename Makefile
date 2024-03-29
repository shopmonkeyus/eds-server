.PHONY: all build e2e lint localstack

all: e2e

build:
	@go build -v -o /dev/null

lint:
	@go fmt ./...

e2e:
	@go run -tags e2e . e2e

e2e_verbose:
	@go run -tags e2e . e2e --verbose

localstack:
	@./hack/localstack