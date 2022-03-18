from pathlib import Path

import pytest

pytest_plugins = ['graph_fixtures', 'interface_fixtures', 'disk_fixtures']


@pytest.fixture
def redis_hostname():
    return 'localhost'


@pytest.fixture
def tests_root():
    return Path(__file__).resolve().parent
