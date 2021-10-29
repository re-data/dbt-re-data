import pytest

def pytest_addoption(parser):
    parser.addoption("--db", action="store")

@pytest.fixture()
def db(pytestconfig):
    return pytestconfig.getoption("db")