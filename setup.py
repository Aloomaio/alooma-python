#!/usr/bin/env python
from distutils.core import setup

setup(
    name='alooma',
    version='0.4.16',
    description='Alooma python API',
    author='Alooma',
    author_email='support@alooma.io',
    packages=['alooma'],
    install_requires=[
      "urllib3>=1.13",
      "requests>=2.9.0",
      "six>=1.4.1"
    ],
    keywords=['alooma', 'api']
)
