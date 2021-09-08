import pytest

pytest_plugins = ['graph_fixtures', 'interface_fixtures', 'disk_fixtures', 'cache_fixtures']
markers = ['redis']


@pytest.fixture
def redis_hostname():
    return 'localhost'
