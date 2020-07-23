from setuptools import setup, find_packages

with open('requirements.txt', encoding='utf-8') as file:
    requirements = file.read().splitlines()

setup(
    name='connectome',
    packages=find_packages(include=('connectome',)),
    include_package_data=True,
    version='0.0.1',
    keywords=[],
    install_requires=requirements,
    python_requires='>=3.6',
)
