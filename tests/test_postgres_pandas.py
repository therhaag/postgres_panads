import pandas as pd
from pandas.testing import assert_frame_equal
from postgres_pandas.db_utils import (db_connection,
                                      update_table_from_df,
                                      upsert_table_from_df)


def test_db_connection(dbinst):
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT version()')
            print(cur.fetchone())


def test_update_table_from_df(test_table, test_data, test_table_as_df_after_update):
    with db_connection() as conn:
        update_table_from_df(test_data,
                             conn,
                             table='demand',
                             keys=['plant_name', 'ph_6_code', 'date'],
                             values=['value', 'pred'])
        conn.commit()
        df = pd.read_sql_query('SELECT * FROM demand', conn)
    assert_frame_equal(df, test_table_as_df_after_update)


def test_upsert_table_from_df(test_table, test_data, test_table_as_df_after_upsert):
    with db_connection() as conn:
        upsert_table_from_df(test_data,
                             conn,
                             table='demand',
                             keys=['plant_name', 'ph_6_code', 'date'],
                             values=['value', 'pred'])
        conn.commit()
        df = pd.read_sql_query('SELECT plant_name, ph_6_code, date, value, pred FROM demand order by value', conn)
    assert_frame_equal(df, test_table_as_df_after_upsert, check_like=True)

