#!/usr/bin/env python
from os import path
from setuptools import setup
from setuptools.command.test import test as TestCommand
import io
import sys

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['tests']
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


def read(filepath):
    return io.open(path.join(path.dirname(__file__), filepath)).read()

setup(
    name='eece411-kvclient',
    version='0.1.0',
    url='',
    license=read('LICENSE'),
    author='Tyler Jones',
    author_email='tyler@squirly.ca',
    description='Key Value client for EECE 411.',
    long_description=read('README.rst'),
    packages=['kvclient', 'testing'],
    install_requires=['gevent'],
    tests_require=['pytest', 'mock'],
    cmdclass={'test': PyTest},
)
