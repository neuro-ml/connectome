import os

import pytest

pytest_plugins = ['graph_fixtures', 'interface_fixtures', 'disk_fixtures']
markers = ['redis']


@pytest.fixture
def redis_hostname():
    if os.environ.get('CI'):
        return 'redis'
    return 'localhost'
