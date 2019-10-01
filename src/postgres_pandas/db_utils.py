import psycopg2
from contextlib import contextmanager


def get_db_config():
    pass


@contextmanager
def db_connection(dsn=None):
    if dsn is None:
        dsn = get_db_config()
    conn = psycopg2.connect(**dsn)
    try:
        yield conn
    finally:
        conn.close()


def update_table_from_df(df, conn, table, keys, values):
    data = ','.join(map(str, (df[keys + values].itertuples(index=False, name=None))))

    value_assign_str = ','.join([f'{v} = new_values.{v}' for v in values])
    where_str = ' AND '.join([f'{table}.{k} = new_values.{k}' for k in keys])
    sql = f'UPDATE {table} ' \
          f'SET ' \
          f'{value_assign_str} ' \
          f'FROM ( ' \
          f'VALUES ' \
          f'{data} ) ' \
          f'AS new_values ({",".join(keys + values)}) ' \
          f'WHERE {where_str}'
    with conn.cursor() as cur:
        cur.execute(sql)
