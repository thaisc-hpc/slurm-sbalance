.DEFAULT: build

.PHONY: build
build: setup.py sbalance/sbalance.py
	python3 -m build

.PHONY: dist
dist: 
	python3 -m twine upload dist/*

.PHONY: test-dist
test-dist:
	python3 -m twine upload --repository testpypi dist/* --verbose

.PHONY: clean
clean:
	rm -r dist
	rm -r build
	rm -r *.egg-info
