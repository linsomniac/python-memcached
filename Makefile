test:
	tox

clean:
	rm -f memcache.pyc memcache.py.orig .tox python_memcached.egg-info

push:
	bzr push lp:python-memcached
