cmd ?= analyzer
version ?= $(shell git describe --tag --dirty)
tags ?= latest $(version)

allCmds := $(shell ls ./cmd/)
dockerimg := livepeer/data

.PHONY: all $(allCmds) docker docker_run docker_push deps_start deps_stop check_local_rabbit test

all: $(allCmds)

$(allCmds):
	$(MAKE) -C ./cmd/$@

run: check_local_rabbit deps_start
	$(MAKE) -C ./cmd/$(cmd) run

docker:
	docker build $(foreach tag,$(tags),-t $(dockerimg):$(tag)) --build-arg version=$(version) .

test:
	go test ./... -v

docker_run: deps_start docker
	docker run -it --rm --name=$(cmd) --entrypoint=./$(cmd) \
		--network=livepeer-data_default -p 8080:8080 \
		-e LP_HOST=0.0.0.0 -e LP_RABBITMQ_URI=amqp://rabbitmq:5672/livepeer \
		$(dockerimg) $(args)

docker_ci:
	docker buildx build --push --platform linux/amd64,linux/arm64 $(foreach tag,$(tags),-t $(dockerimg):$(tag)) --build-arg version=$(version) .

deps_start:
	docker-compose up -d
	@printf 'Waiting rabbitmq healthy ...'
	@until docker inspect --format='{{json .State.Health.Status}}' rabbitmq | grep -q '"healthy"' ; \
	do \
		printf '.' && sleep 1 ; \
	done;
	@printf " \e[32mdone\e[m\n"


deps_stop:
	docker-compose down

check_local_rabbit:
	@cat /etc/hosts | grep -wq rabbitmq || { \
		echo "Host for local rabbitmq not configured in /etc/hosts."; \
		echo "To run locally, configure it with:"; \
		echo "> echo '127.0.0.1 rabbitmq' >> /etc/hosts"; \
		exit 1; \
	}

.PHONY: release
release:
	@if [[ ! "$(version)" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-.+)?$$ ]]; then \
		echo "Must provide semantic version as arg to make." ; \
		echo "e.g. make release version=1.2.3-beta" ; \
		exit 1 ; \
	fi
	@git diff --quiet || { echo "Git working directory is dirty."; exit 1 ; }

	git tag -a v$(version) -m "Release v$(version)"
	git push origin v$(version)
