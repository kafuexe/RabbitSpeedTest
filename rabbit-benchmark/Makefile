# RabbitMQ client benchmark suite
#
# On Windows the venv python lives under .venv/Scripts; on POSIX under .venv/bin.
ifeq ($(OS),Windows_NT)
PYTHON ?= .venv/Scripts/python
else
PYTHON ?= .venv/bin/python
endif

OUTPUT_DIR ?= results

.PHONY: help install test run-fake run-local run-local-full

help:
	@echo "Targets:"
	@echo "  make install          Install dependencies into the active environment"
	@echo "  make test             Run the test suite (no broker needed)"
	@echo "  make run-fake         Broker-free run using the in-memory fake client"
	@echo "  make run-local        Quick run vs local RabbitMQ (guest/guest@localhost) via configs/smoke.json"
	@echo "  make run-local-full   Full-defaults run vs local RabbitMQ (guest/guest@localhost)"

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(PYTHON) -m pytest -q

# Broker-free sanity run (in-memory fake client) — same as the quick smoke run.
run-fake:
	$(PYTHON) -m benchmark.main --clients fake --message-count 200 --iterations 3 --output-dir $(OUTPUT_DIR)

# Quick run against a local RabbitMQ at amqp://guest:guest@localhost:5672/
# (the suite's default URL). Uses a trimmed config so it finishes fast.
run-local:
	$(PYTHON) -m benchmark.main --config configs/smoke.json --output-dir $(OUTPUT_DIR)

# Full run against local RabbitMQ with the suite defaults (large; takes a while).
run-local-full:
	$(PYTHON) -m benchmark.main --clients pika,aio-pika --output-dir $(OUTPUT_DIR)
