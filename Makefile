build:
	rm -rf dist
	python3 setup.py sdist

test:
	coverage run --concurrency=multiprocessing -m pytest tests -vx || exit 1
	coverage combine
	coverage report --include="mongita/*.py,mongitasync"
	coverage html --include="mongita/*.py,mongitasync"

loc_count_all:
	pygount --format=summary . --suffix=py

loc_count:
	pygount --format=summary mongita --suffix=py

benchmark:
	python3 benchmark_tests/benchmark.py
