from postgres_pandas.db_utils import db_connection, update_table_from_df


def test_db_connection(dbinst):
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT version()')
            print(cur.fetchone())


def test_update_table_from_df(test_table, test_data):
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM demand')
            print(cur.fetchall())
        update_table_from_df(test_data,
                             conn,
                             table='demand',
                             keys=['plant_name', 'ph_6_code', 'date'],
                             values=['value', 'pred'])
        conn.commit()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM demand')
            print(cur.fetchall())

