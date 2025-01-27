CONFIG_PATH := $(USERPROFILE)\proglog

.PHONY: init

init:
	mkdir ${CONFIG_PATH}

.PHONY: gencert

gencert:
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
 		test/client-csr.json | cfssljson -bare root-client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
 		test/client-csr.json | cfssljson -bare nobody-client
	move *.pem ${CONFIG_PATH}
	move *.csr ${CONFIG_PATH}

$(CONFIG_PATH)\model.conf:
	copy test\model.conf "$(CONFIG_PATH)\model.conf"

$(CONFIG_PATH)\policy.csv:
	copy test\policy.csv "$(CONFIG_PATH)\policy.csv"

.PHONY: genacl
genacl: $(CONFIG_PATH)\model.conf $(CONFIG_PATH)\policy.csv
	echo "Access control lists configured."

.PHONY: test
test:
	go test -race ./...

.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

TAG ?= 0.0.1

build-docker:
	docker build -t adeeshajayasinghe/dls:$(TAG) .