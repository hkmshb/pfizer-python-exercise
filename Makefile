.PHONY: clean lint run test

clean:
	rm -rf ./bin/uploads.db3

lint:
	pre-commit run --all-files

run:
	python handler.py

test:
	pytest
