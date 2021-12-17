build:
	rm -rf dist
	python3 setup.py sdist

# Coverage has to be run in this weird way. Why? mongoengine and internal python
# weirdness. Mongoengine relies on the value of pymongo.MongoClient to be set
# to either pymongo native or mongoengine BEFORE it gets imported. After import,
# mongoengine can't be deleted or reloaded or anything afterwards. So it needs to be
# run in different coverage runs. Then, the --concurrency flag needs to be set to
# have the coverage actually recorded before coverage combine.
# For the record, this is horrid.
test:
	coverage run --concurrency=multiprocessing -m pytest tests/test_mongita.py -vx || exit 1
	coverage run --concurrency=multiprocessing --append -m pytest tests/test_mongoengine.py -vx || exit 1
	coverage run --concurrency=multiprocessing --append -m pytest tests/test_mongoengine_disk.py -vx || exit 1
	coverage combine
	coverage report --include="mongita/*.py,mongitasync"
	coverage html --include="mongita/*.py,mongitasync"

loc_count_all:
	pygount --format=summary . --suffix=py

loc_count:
	pygount --format=summary mongita --suffix=py

benchmark:
	python3 benchmark_tests/benchmark.py
