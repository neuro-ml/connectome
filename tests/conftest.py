import pytest

pytest_plugins = ['graph_fixtures', 'interface_fixtures', 'hash_fixtures']
markers = ['redis']


@pytest.fixture
def redis_hostname():
    return 'redis'
