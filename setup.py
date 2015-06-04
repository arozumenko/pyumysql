#!env python
from setuptools import setup

install_requires = [
    'umysql'
]

version = '0.1a'

setup(name='pyumysql',
      version=version,
      packages=[
          'pyumysql',
          'pyumysql.constants'],
      install_requires=install_requires,
      # Metadata.
      description='Wrapper for ultamysql to provide minimum set of pymysql '
                  'libs',
      author='Artem Rozumenko',
      author_email='artyom.rozumenko@gmail.com',
      license='Apache Software License')
