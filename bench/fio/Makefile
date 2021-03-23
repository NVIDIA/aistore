# Run full FIO benchmark on disk $FSPATH, output results to $OUT
#
# Example:
# $ FSPATH="/ais/sda" OUT="~/fio.out" make bench
#
# See: https://linux.die.net/man/1/fio
bench:
	@if [ -z "${FSPATH}" ]; then echo "FSPATH variable cannot be empty"; exit 1; fi
	@if [ -z "${OUT}" ]; then echo "OUT variable cannot be empty"; exit 1; fi
	fio fio.cfg --directory="${FSPATH}" --output="${OUT}"
	@echo "Remember to clean up created files in ${FSPATH} directory"

dependencies:
	sudo apt-get install fio

