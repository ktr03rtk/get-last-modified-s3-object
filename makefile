build:
	sam build

test:
	@cd ./get-last-modified-s3-object/ && \
	go test -v ./...

run: build
	sam local invoke \
		--env-vars env.json -e event.json

up: build
	sam local start-api \
		--env-vars env.json -e event.json

deploy: build
	sam deploy --guided

.PHONY: build test local-run local-up dev-deploy
