#!/usr/bin/env python

from setuptools import setup  # noqa
from setuptools.depends import get_module_constant

dl_url = "https://github.com/linsomniac/python-memcached/releases/download/{0}/python-memcached-{0}.tar.gz"

version = get_module_constant("memcache", "__version__")
setup(
    name="python-memcached",
    version=version,
    description="Pure python memcached client",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Evan Martin",
    author_email="martine@danga.com",
    maintainer="Sean Reifschneider",
    maintainer_email="jafo00@gmail.com",
    url="https://github.com/linsomniac/python-memcached",
    download_url="https://github.com/linsomniac/python-memcached/releases/download/{0}/python-memcached-{0}.tar.gz".format(
        version
    ),  # noqa
    py_modules=["memcache"],
    install_requires=open("requirements.txt").read().split(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Python Software Foundation License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
)
