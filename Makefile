# Build output knobs (override via env/CLI).
EXTENSION     ?= 
DIST_DIR      ?= dist/
GOOS          ?= linux
GOARCH        ?= $(shell go env GOARCH)
BUILDINFOSDET ?= 

NAME          := goflow2
DOCKER_IMAGE  ?= netsampler/$(NAME)
VERSION       ?= $(shell git describe --tags --abbrev --long HEAD 2>/dev/null || git rev-parse --short HEAD)
ABBREV        ?= $(shell git rev-parse --short HEAD)
COMMIT        ?= $(shell git rev-parse HEAD)
TAG           ?= $(shell git describe --tags --abbrev=0 HEAD 2>/dev/null || echo dev)
VERSION_PKG   ?= $(shell echo $(VERSION) | sed 's/^v//g')
LICENSE       := BSD-3-Clause
URL           := https://github.com/netsampler/goflow2
DESCRIPTION   := GoFlow2: Open-Source and Scalable Network Sample Collector
DATE          :=  $(shell date +%FT%T%z)
BUILDINFOS    ?=  ($(DATE)$(BUILDINFOSDET))
LDFLAGS       ?= '-X main.version=$(VERSION) -X main.buildinfos=$(BUILDINFOS)'
MAINTAINER    := lspgn@users.noreply.github.com
DOCKER_BIN    ?= docker
DOCKER_CMD    ?= buildx build
DOCKER_SUFFIX ?= 
DOCKER_IMAGE_PREFIXES ?= $(DOCKER_IMAGE)
DOCKER_TAGS ?= $(foreach image,$(DOCKER_IMAGE_PREFIXES),$(image):$(ABBREV)$(DOCKER_SUFFIX))
DOCKER_TAG_ARGS := $(foreach tag,$(DOCKER_TAGS),-t $(tag))
DOCKER_MANIFEST_TAG ?= $(ABBREV)

OUTPUT := $(DIST_DIR)goflow2-$(VERSION_PKG)-$(GOOS)-$(GOARCH)$(EXTENSION)

# fpm expects x86_64 for amd64, but use GOARCH otherwise.
FPM_ARCH ?= $(GOARCH)
ifeq ($(GOARCH),amd64)
FPM_ARCH = x86_64
endif

.PHONY: proto
# Generate Go protobuf bindings (requires protoc).
proto:
	@echo generating protobuf
	protoc --go_opt=paths=source_relative --go_out=. pb/*.proto
	protoc --go_opt=paths=source_relative --go_out=. cmd/enricher/pb/*.proto

.PHONY: vet
# Run Go static analysis on the main package.
vet:
	go vet cmd/goflow2/main.go

.PHONY: test
# Run Go test suite.
test:
	go test -v ./...

.PHONY: test-race
test-race:
	go test -race ./...

.PHONY: test-cover
test-cover:
	go test -cover ./...

.PHONY: staticcheck
staticcheck:
	staticcheck ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: check
check: vet test lint staticcheck

.PHONY: prepare
# Ensure dist directory exists.
prepare:
	mkdir -p $(DIST_DIR)

.PHONY: clean
clean:
	rm -rf $(DIST_DIR)

.PHONY: build
# Build the goflow2 binary (bare `make` also runs this).
.DEFAULT_GOAL := build
build: prepare
	CGO_ENABLED=0 go build -ldflags $(LDFLAGS) -o $(OUTPUT) cmd/goflow2/main.go

.PHONY: print-output
print-output:
	@echo $(OUTPUT)

.PHONY: docker
# Build docker image for the current version.
docker:
	$(DOCKER_BIN) $(DOCKER_CMD) \
        --build-arg LDFLAGS=$(LDFLAGS) \
        --build-arg CREATED="$(DATE)" \
        --build-arg MAINTAINER="$(MAINTAINER)" \
        --build-arg URL="$(URL)" \
        --build-arg NAME="$(NAME)" \
        --build-arg DESCRIPTION="$(DESCRIPTION)" \
        --build-arg LICENSE="$(LICENSE)" \
        --build-arg VERSION="$(VERSION)" \
        --build-arg REV="$(COMMIT)" \
        $(DOCKER_TAG_ARGS) .

.PHONY: push-docker
# Push docker image tagged with the current abbrev.
push-docker:
	@for tag in $(DOCKER_TAGS); do \
		$(DOCKER_BIN) push $$tag; \
	done

.PHONY: docker-manifest
# Create and push multi-arch manifest for abbrev and latest tags.
docker-manifest:
	$(DOCKER_BIN) buildx imagetools create \
	    -t $(DOCKER_IMAGE):$(DOCKER_MANIFEST_TAG) \
	    $(DOCKER_IMAGE):$(ABBREV)-amd64 \
	    $(DOCKER_IMAGE):$(ABBREV)-arm64

.PHONY: package-deb
package-deb: build
	$(call run_fpm,deb)

.PHONY: package-rpm
package-rpm: build
	$(call run_fpm,rpm)

.PHONY: package
package: package-deb package-rpm

FPM_COMMON_FLAGS := -s dir -n $(NAME) -v $(VERSION_PKG) \
	--maintainer "$(MAINTAINER)" \
	--description "$(DESCRIPTION)" \
	--url "$(URL)" \
	--architecture $(FPM_ARCH) \
	--license "$(LICENSE)" \
	--package $(DIST_DIR)
FPM_INPUTS := \
	$(OUTPUT)=/usr/bin/goflow2 \
	package/goflow2.service=/lib/systemd/system/goflow2.service \
	package/goflow2.env=/etc/default/goflow2

define run_fpm
	fpm -t $(1) $(FPM_COMMON_FLAGS) $(FPM_INPUTS)
endef

# ─────────────────────────────────────────────
# Deploy package: tar.gz containing binaries + scripts + conf
# ─────────────────────────────────────────────
RELEASE_DIR ?= dist
DEPLOY_ARCH  ?= all

.PHONY: package-deploy
package-deploy:
	@$(MAKE) deploy-package ARCH=$(DEPLOY_ARCH)

.PHONY: deploy-package
deploy-package:
	@echo "=== GoFlow2 deploy package build ==="
	@echo "version : $(TAG)"
	@echo "date    : $(DATE)"
	@echo "arch    : $(DEPLOY_ARCH)"
	@mkdir -p $(RELEASE_DIR)/bin $(RELEASE_DIR)/conf
	@case "$(DEPLOY_ARCH)" in \
		amd64|x86_64)  $(MAKE) deploy-arch GOARCH=amd64 OUT_BIN=goflow2-linux-amd64 ENRICH_BIN=enricher-goflow2-linux-amd64 ;; \
		arm64|aarch64) $(MAKE) deploy-arch GOARCH=arm64 OUT_BIN=goflow2-linux-arm64 ENRICH_BIN=enricher-goflow2-linux-arm64 ;; \
		all) \
			$(MAKE) deploy-arch GOARCH=amd64 OUT_BIN=goflow2-linux-amd64 ENRICH_BIN=enricher-goflow2-linux-amd64 ; \
			$(MAKE) deploy-arch GOARCH=arm64 OUT_BIN=goflow2-linux-arm64 ENRICH_BIN=enricher-goflow2-linux-arm64 ; \
			;; \
		*) echo "unknown arch: $(DEPLOY_ARCH) (use amd64|arm64|all)"; exit 1 ;; \
	esac
	@cp .deploy/conf/* $(RELEASE_DIR)/conf/
	@PKG_DIR="goflow2-deploy-$(TAG)-$(shell date +%Y%m%d-%H%M%S)"; \
	 mkdir -p $(RELEASE_DIR)/"$$PKG_DIR"; \
	 cp -r $(RELEASE_DIR)/bin $(RELEASE_DIR)/"$$PKG_DIR"/; \
	 cp -r $(RELEASE_DIR)/conf $(RELEASE_DIR)/"$$PKG_DIR"/; \
	 cp .deploy/run.sh .deploy/setenv.sh $(RELEASE_DIR)/"$$PKG_DIR"/ 2>/dev/null || true; \
	 cp .deploy/README.md $(RELEASE_DIR)/"$$PKG_DIR"/README.md 2>/dev/null || true; \
	 (cd $(RELEASE_DIR) && tar czf "$$PKG_DIR.tar.gz" "$$PKG_DIR"); \
	 echo ">>> done  $(RELEASE_DIR)/$$PKG_DIR.tar.gz"
	@ls -lh $(RELEASE_DIR)/goflow2-deploy-*-*.tar.gz

.PHONY: deploy-arch
deploy-arch:
	@echo ">>> building $(OUT_BIN) ..."
	@GOOS=linux GOARCH=$(GOARCH) CGO_ENABLED=0 go build \
		-ldflags $(LDFLAGS) \
		-o $(RELEASE_DIR)/bin/$(OUT_BIN) \
		./cmd/goflow2/main.go
	@GOOS=linux GOARCH=$(GOARCH) CGO_ENABLED=0 go build \
		-ldflags $(LDFLAGS) \
		-o $(RELEASE_DIR)/bin/$(ENRICH_BIN) \
		./cmd/enricher/main.go
	@echo ">>> done $(OUT_BIN)"

.PHONY: deploy-clean
deploy-clean:
	rm -rf $(RELEASE_DIR)
