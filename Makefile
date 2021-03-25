test:
	coverage run --concurrency=multiprocessing -m pytest tests -vx || exit 1
	coverage combine
	coverage report --include=mongita/*.py
	coverage html --include=mongita/*.py

loc_count:
	pygount --format=summary . --suffix=py
