import runpy
from pathlib import Path

from setuptools import setup, find_packages

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3 :: Only',
]

name = 'connectome'
root = Path(__file__).parent
with open(root / 'requirements.txt', encoding='utf-8') as file:
    requirements = file.read().splitlines()
with open(root / 'README.md', encoding='utf-8') as file:
    long_description = file.read()
version = runpy.run_path(root / name / '__version__.py')['__version__']

setup(
    name=name,
    packages=find_packages(include=(name,)),
    include_package_data=True,
    version=version,
    description='A library for datasets containing heterogeneous data',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/neuro-ml/connectome',
    download_url='https://github.com/neuro-ml/connectome/archive/v%s.tar.gz' % version,
    keywords=['dag', 'dataset', 'cache', 'consistency'],
    classifiers=classifiers,
    install_requires=requirements,
    python_requires='>=3.6',
)
