test:
	python memcache.py
	( cd tests; make )

clean:
	rm -f memcache.pyc memcache.py.orig

push:
	bzr push lp:python-memcached
