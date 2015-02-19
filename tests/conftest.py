import pytest

from tests.mock_config import mock_conf_files


@pytest.yield_fixture
def mock_files():
    with mock_conf_files():
        yield
