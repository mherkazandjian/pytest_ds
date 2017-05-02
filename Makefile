.PHONY: nothing clean

nothing:

build_all:
	python2 setup.py build
	python2 setup.py sdist bdist_wheel


install: build_all
	python2 setup.py install

clean:
	rm -fvr *.egg* build dist *~

#bootstrap-dev:
#	cd tests/dev_config && ./bootstrap_data.sh

#dev: bootstrap-dev build_all

#all: dev
