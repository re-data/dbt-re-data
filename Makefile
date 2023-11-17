
# Maybe change this if you are not running on a Mac
CONTAINER_ARCH = linux/amd64

.PHONY: help run-all-ci test-postgres generate-docs

help:
	$(info ${HELP_MESSAGE})
	@exit 0


# Run GitHub Actions CI jobs locally
run-all-ci: test-postgres generate-docs
	@echo "All CI steps completed."

test-postgres:
	@echo "Running test-postgres job..."
	act -j test-postgres --container-architecture $(CONTAINER_ARCH)

generate-docs:
	@echo "Running generate-docs job..."
	act -j generate-docs --container-architecture $(CONTAINER_ARCH)


define HELP_MESSAGE
Usage: $ make [TARGETS]

TARGETS
  help                   Shows this help message
  run-all-ci             Runs all CI steps
  test-postgres          Runs test-postgres job
  generate-docs          Generates documentation

endef
