#!/usr/bin/env python
from distutils.core import setup
try:
      from pip.req import parse_requirements
except ImportError:
      from pip._internal.req import parse_requirements

install_reqs = parse_requirements("requirements.txt", session=False)

reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='alooma',
    version='0.4.13',
    description='Alooma python API',
    author='Alooma',
    author_email='support@alooma.io',
    packages=['alooma'],
    install_requires=reqs,
    keywords=['alooma', 'api']
)
