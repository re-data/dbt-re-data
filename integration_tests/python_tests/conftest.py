import pytest

def pytest_addoption(parser):
    parser.addoption("--db", action="store")
    parser.addoption("--source_schema", action="store")
    

@pytest.fixture()
def db(pytestconfig):
    return pytestconfig.getoption("db")

@pytest.fixture()
def source_schema(pytestconfig):
    return pytestconfig.getoption("source_schema")