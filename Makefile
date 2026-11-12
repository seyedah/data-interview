# Convenience wrapper around the pipeline CLI.
#   make run         full pipeline (ingest -> validate -> clean -> aggregate -> load)
#   make analytics   run the analytical queries, save CSV reports to reports/
#   make test        run the pytest suite
#   make reset-db    delete the local SQLite database (fresh start)
#
# NOTE: `clean` is the pipeline's data-cleaning step (raw -> staging),
# not a build-artifact cleaner; use `reset-db` to remove the database.

PYTHON := $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)
CSV ?= network_logs_dirty.csv
DB ?= data/warehouse.sqlite

.PHONY: help setup run ingest validate clean aggregate load analytics test status reset-db

help:
	@echo "Targets: setup run ingest validate clean aggregate load analytics test status reset-db"

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

run:
	$(PYTHON) -m pipeline run --csv $(CSV) --db $(DB)

ingest:
	$(PYTHON) -m pipeline ingest --csv $(CSV) --db $(DB)

validate:
	$(PYTHON) -m pipeline validate --db $(DB)

clean:
	$(PYTHON) -m pipeline clean --db $(DB)

aggregate:
	$(PYTHON) -m pipeline aggregate --db $(DB)

load:
	$(PYTHON) -m pipeline load --db $(DB)

analytics:
	$(PYTHON) -m pipeline analytics --db $(DB)

status:
	$(PYTHON) -m pipeline status --db $(DB)

test:
	$(PYTHON) -m pytest -v

reset-db:
	rm -f $(DB) $(DB)-wal $(DB)-shm
