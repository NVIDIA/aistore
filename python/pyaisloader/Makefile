BUCKET ?= ais://pyaisloader

.PHONY: install
install:
	pip install -e .

.PHONY: short_put short_get short_get_etl short_mixed short_mixed_etl short_list short_ais_dataset short_ais_iter_dataset
short_put: ## Run a short PUT benchmark
	pyaisloader PUT --bucket $(BUCKET) --duration 30s --workers 16 --totalsize 1GB --minsize 5KB --maxsize 10KB --cleanup

short_get: ## Run a short GET benchmark
	pyaisloader GET --bucket $(BUCKET) --duration 30s --workers 16 --totalsize 1GB --minsize 5KB --maxsize 10KB --cleanup

short_get_etl: ## Run a short GET benchmark where all GETs undergo the ECHO ETL transformation
	pyaisloader GET --bucket $(BUCKET) --duration 30s --workers 16 --totalsize 1GB --minsize 5KB --maxsize 10KB --etl ECHO --cleanup

short_mixed: ## Run a short MIXED benchmark
	pyaisloader MIXED --bucket $(BUCKET) --duration 30s --workers 16 --minsize 5KB --maxsize 10KB --putpct 50 --cleanup

short_mixed_etl: ## Run a short MIXED benchmark where all GETs undergo the ECHO ETL transformation
	pyaisloader MIXED --bucket $(BUCKET) --duration 30s --workers 16 --minsize 5KB --maxsize 10KB --putpct 50 --etl ECHO --cleanup

short_list: ## Run a short LIST benchmark
	pyaisloader LIST --bucket $(BUCKET) --cleanup true --objects 50000 --workers 16

short_ais_dataset: ## Run a short AISDataset benchmark
	pyaisloader AIS_DATASET --bucket $(BUCKET) --duration 30s --workers 16 --totalsize 1GB --minsize 5KB --maxsize 10KB --cleanup

short_ais_iter_dataset: ## Run a short AISIterDataset benchmark
	pyaisloader AIS_ITER_DATASET --bucket $(BUCKET) --iterations 1 --duration 30s --workers 16 --totalsize 1GB --minsize 5KB --maxsize 10KB --cleanup

.PHONY: long_put long_get long_mixed long_list
long_put: ## Run a long (and more intensive) PUT benchmark
	pyaisloader PUT --bucket $(BUCKET) --duration 30m --workers 32 --totalsize 10GB --minsize 50MB --maxsize 100MB --cleanup

long_get: ## Run a long (and more intensive) GET benchmark
	pyaisloader GET --bucket $(BUCKET) --duration 30m --workers 32 --totalsize 10GB --minsize 5KB --maxsize 10KB --cleanup

long_mixed: ## Run a long (and more intensive) MIXED benchmark
	pyaisloader MIXED --bucket $(BUCKET) --duration 30m --workers 32 --totalsize 10GB --minsize 5KB --maxsize 10KB --putpct 50 --cleanup

long_list: ## Run a long (and more intensive) LIST benchmark
	pyaisloader LIST --bucket $(BUCKET) --objects 500000 --workers 32 --cleanup

.PHONY: ci-short ci-long ci-etl
ci-short: install
	yes "y" | head -n 2 | pyaisloader p -b $(BUCKET) -d 15s -min 1mb -max 10mb -s 1gb -w 16
	yes "y" | head -n 2 | pyaisloader g -b $(BUCKET) -d 15s -min 1mb -max 10mb -s 1gb -w 16
	yes "y" | head -n 2 | pyaisloader m -b $(BUCKET) -d 15s -min 1mb -max 10mb -w 16 -c
	yes "y" | head -n 2 | pyaisloader ais_dataset -b $(BUCKET) -d 15s -min 1mb -max 10mb -w 16 -c
	yes "y" | head -n 2 | pyaisloader ais_iter_dataset -b $(BUCKET) -i 1 -d 15s -min 1mb -max 10mb -w 16 -c

ci-long: install
	yes "y" | head -n 2 | pyaisloader p -b $(BUCKET) -d 3m -min 1mb -max 10mb -s 10gb -w 16
	yes "y" | head -n 2 | pyaisloader g -b $(BUCKET) -d 3m -min 1mb -max 10mb -s 10gb -w 16
	yes "y" | head -n 2 | pyaisloader m -b $(BUCKET) -d 3m -min 1mb -max 10mb -w 16 -c

ci-etl: install
	yes "y" | head -n 2 | pyaisloader m -b $(BUCKET) -d 1m -min 1mb -max 10mb -w 16 --etl ECHO
	yes "y" | head -n 2 | pyaisloader m -b $(BUCKET) -d 1m -min 1mb -max 10mb -w 16 --etl MD5 -c

.PHONY: help
help:
	@echo -e "\n\e[1mUsage:\e[0m make <target>\n"
	@echo -e "\e[1mTargets:\e[0m"
	@egrep '^(.+)\:\ ##\ .+' ${MAKEFILE_LIST} | column -t -c 2 -s ':#' | awk 'BEGIN {FS = ":"} {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo -e "\n"
