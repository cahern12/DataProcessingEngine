.PHONY: test fixtures demo

test:
	python3 -m unittest discover -s tests -v

fixtures:
	python3 -m dpe check-fixtures

demo:
	python3 -m dpe demo
