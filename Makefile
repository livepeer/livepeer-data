GH_REF := $(shell echo $${GITHUB_HEAD_REF:-$$GITHUB_REF})
ifeq ($(GH_REF),$(GH_REF:refs/heads/%=%)) # Ignore ref without refs/heads prefix
	GH_REF := $(shell git branch --points-at HEAD | sed -e "s/^[\* ]*//" -e '/HEAD detached/d')
else
	GH_REF := $(GH_REF:refs/heads/%=%)
endif
branches := $(foreach branch,$(GH_REF),$(shell echo '$(branch)' | sed 's/\//-/g' | tr -cd '[:alnum:]_-'))

version ?= $(shell git describe --tag --dirty)
cmd ?= analyzer

allCmds := $(shell ls ./cmd/)
dockerimg := livepeer/data
dockertags := latest $(branches) $(version)

.PHONY: all $(allCmds) docker docker_run docker_push deps_start deps_stop check_local_rabbit

all: $(allCmds)

$(allCmds):
	$(MAKE) -C ./cmd/$@

run: check_local_rabbit deps_start
	$(MAKE) -C ./cmd/$(cmd) run

docker:
	git branch -a --points-at HEAD
	docker build $(foreach tag,$(dockertags),-t $(dockerimg):$(tag)) --build-arg version=$(version) .

docker_run: deps_start docker
	docker run -it --rm --name=$(cmd) --entrypoint=./$(cmd) \
		--network=livepeer-data_default -p 8080:8080 \
		-e LP_HOST=0.0.0.0 -e LP_RABBITMQ_URI=amqp://rabbitmq:5672/livepeer \
		$(dockerimg) $(args)

docker_push:
	git branch -a --points-at HEAD
	git branch -a
	for TAG in $(dockertags) ; \
	do \
		docker push $(dockerimg):$$TAG ; \
	done;

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
