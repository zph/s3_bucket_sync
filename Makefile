run:
	bash -c 'go run src/s3sync/s3sync.go -source $$S3_SOURCE -destination $$S3_DESTINATION'
build:
	bash -c 'GOPATH=$$(pwd) gb build all'
clean:
	rm -rf ./bin/*
