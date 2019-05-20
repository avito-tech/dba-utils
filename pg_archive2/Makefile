SHELL          := bash
PGVER          := 9.6
IMAGE_NAME     := postgresql-test
CONTAINER_NAME := test
CONTAINERS     := $(CONTAINER_NAME) test-archive02 test-archive03
APP_DIR        := /app

DBUSER  := postgres
DBNAME  := test
NOTERM  ?= -T

SRC_APP       := $(wildcard *_2) wal-sync
SRC_SQL       := backup_queue.sql
APP_TESTS_DIR := tests
APP_TESTS     := $(wildcard $(APP_TESTS_DIR)/*.bats $(APP_TESTS_DIR)/*.bash)

export IMAGE_NAME APP_DIR

all: test

.PHONY: help test test-app clean docker-clean distclean

help:
	@echo "DBG $(SRC_APP)"
	@echo "help         - show this help"
	@echo "docker-build - build docker image"
	@echo "docker-clean - stop container and remove image"
	@echo "clean        - remove artifacts (except image)"
	@echo "distclean    - remove all artifacts"
	@echo "test         - run tests"
	@echo "test-app     - run app tests only"

clean: .stopped
	docker-compose down -v -t1
	rm -f .started .stopped .migration

docker-clean: .stopped
	docker rmi -f $(IMAGE_NAME)
	@echo
	rm -f docker-build

distclean: clean docker-clean

docker-build: Dockerfile
	docker build --build-arg PGVER=$(PGVER) --tag $(IMAGE_NAME) .
	rm -f .stopped
	@date > $@

.started: docker-build .stopped
	docker-compose up -d
	for host in $(CONTAINERS); do \
		echo "wait for $$host to start ..."; \
		for ((i = 10; i > 0; i--)); do \
			[[ $$(docker-compose logs --tail 10 $$host) = *'= docker-entrypoint.sh started ='* ]] && break; \
			sleep 1; \
		done; \
		(( i )) || { echo "$$host start timeout"; docker logs $$host; exit 1; }; \
	done
	@date > $@

.stopped:
	docker-compose down -v -t1
	rm -f .started
	@date > $@

test: .migration test-app

.migration: .started $(SRC_SQL)
	docker-compose exec $(NOTERM) $(CONTAINER_NAME) psql -X -At -Ppager=off -vON_ERROR_STOP=1 -1 -f "$(SRC_SQL)" test
	@date > $@

test-app: .started $(SRC_APP) $(APP_TESTS)
	docker-compose exec $(NOTERM) $(CONTAINER_NAME) bash -c 'bats $(APP_DIR)/$(APP_TESTS_DIR)'
