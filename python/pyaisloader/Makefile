install:
	pip install -r requirements.txt
	pip install -e .

short_put: ## Run a short PUT benchmark
	pyaisloader PUT --bucket ais://abc --duration 30s --workers 16 --cleanup true --totalsize 1GB --minsize 5KB --maxsize 10KB

short_get: ## Run a short GET benchmark
	pyaisloader GET --bucket ais://abc --duration 30s --workers 16 --cleanup true --totalsize 1GB --minsize 5KB --maxsize 10KB

short_mixed: ## Run a short MIXED benchmark
	pyaisloader MIXED --bucket ais://abc --duration 30s --workers 16 --cleanup true --minsize 5KB --maxsize 10KB --putpct 50

short_list: ## Run a short LIST benchmark
	pyaisloader LIST --bucket ais://abc --cleanup true --objects 50000 --workers 16

long_put: ## Run a long (and more intensive) PUT benchmark
	pyaisloader PUT --bucket ais://abc --duration 30m --workers 32 --cleanup true --totalsize 10GB --minsize 50MB --maxsize 100MB

long_get: ## Run a long (and more intensive) GET benchmark
	pyaisloader GET --bucket ais://abc --duration 30m --workers 32 --cleanup true --totalsize 10GB --minsize 5KB --maxsize 10KB

long_mixed: ## Run a long (and more intensive) MIXED benchmark
	pyaisloader MIXED --bucket ais://abc --duration 30m --workers 32 --cleanup true --totalsize 10GB --minsize 5KB --maxsize 10KB --putpct 50

long_list: ## Run a long (and more intensive) LIST benchmark
	pyaisloader LIST --bucket ais://abc --cleanup true --objects 500000 --workers 32

help:
	@echo -e "\n\e[1mUsage:\e[0m make <target>\n"
	@echo -e "\e[1mTargets:\e[0m"
	@egrep '^(.+)\:\ ##\ .+' ${MAKEFILE_LIST} | column -t -c 2 -s ':#' | awk 'BEGIN {FS = ":"} {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo -e "\n"
