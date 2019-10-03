import pandas as pd
from pytest_postgresql import factories
from postgres_pandas import db_utils
import pytest


pginst = factories.postgresql_proc('/usr/local/bin/pg_ctl')
dbinst = factories.postgresql('pginst', 'test_db')


@pytest.fixture
def dsn(pginst):
    return {'host': pginst.host,
            'port': pginst.port,
            'user': pginst.user}


@pytest.fixture(autouse=True)
def use_test_db(monkeypatch, dsn):

    def mock_config():
        return {**dsn, 'dbname': 'test_db'}

    monkeypatch.setattr(db_utils, 'get_db_config', mock_config)


@pytest.fixture
def test_data():
    df = pd.DataFrame({'plant_name': ['p1', 'p2', 'p3'],
                       'ph_6_code': ['0815', '0815', '0815'],
                       'date': ['2019-01-01', '2019-01-01', '2019-01-01'],
                       'value': [42.0, 5.0, 23.0],
                       'pred': [45.0, 5.0, 25.0]})
    return df

@pytest.fixture
def test_db(dbinst):
    with dbinst.cursor() as cur:
        cur.execute('CREATE TABLE demand '
                    '(actuals_id SERIAL NOT NULL PRIMARY KEY, '
                    'plant_name varchar NOT NULL, '
                    'ph_6_code varchar NOT NULL, '
                    'date varchar NOT NULL, '
                    'value decimal(10,2), '
                    'pred decimal(10,2))')
    dbinst.commit()
    return dbinst


@pytest.fixture
def test_table(test_db):
    with test_db.cursor() as cur:
        cur.execute("INSERT INTO demand (plant_name, ph_6_code, date, value, pred) "
                    "VALUES ('p1', '0815', '2019-01-02', 666, 777)")
        cur.execute("INSERT INTO demand (plant_name, ph_6_code, date, value, pred) "
                    "VALUES ('p2', '0815', '2019-01-01', 666, 777)")
        cur.execute('ALTER TABLE demand ADD CONSTRAINT "demand_unique_ppd" UNIQUE ("plant_name", "ph_6_code", "date")')
    test_db.commit()
    return test_db


@pytest.fixture
def test_table_as_df_after_update():
    df = pd.DataFrame({'actuals_id': [1, 2],
                       'plant_name': ['p1', 'p2'],
                       'ph_6_code': ['0815', '0815'],
                       'date': ['2019-01-02', '2019-01-01'],
                       'value': [666.0, 5.0],
                       'pred': [777.0, 5.0]})
    return df


@pytest.fixture
def test_table_as_df_after_upsert():
    df = pd.DataFrame({'plant_name': ['p2', 'p3', 'p1', 'p1'],
                       'ph_6_code': ['0815', '0815', '0815', '0815'],
                       'date': ['2019-01-01', '2019-01-01', '2019-01-01', '2019-01-02'],
                       'value': [5.0, 23.0, 42.0, 666.0],
                       'pred': [5.0, 25.0, 45.0, 777]})
    return df
