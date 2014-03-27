#!/usr/bin/env python

from setuptools import setup  # noqa


setup(name="python-memcached",
      version="1.53",
      description="Pure python memcached client",
      long_description=open("README.md").read(),
      author="Evan Martin",
      author_email="martine@danga.com",
      maintainer="Sean Reifschneider",
      maintainer_email="jafo@tummy.com",
      url="http://www.tummy.com/Community/software/python-memcached/",
      download_url="ftp://ftp.tummy.com/pub/python-memcached/",
      py_modules=["memcache"],
      install_requires=open('requirements.txt').read().split(),
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: Python Software Foundation License",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Topic :: Internet",
          "Topic :: Software Development :: Libraries :: Python Modules",
      ])
