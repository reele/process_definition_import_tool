import psycopg2
import ds_config

def get_db_connection():
    return psycopg2.connect(
        database=ds_config.DB_DATABASE,
        host=ds_config.DB_HOST,
        port=ds_config.DB_PORT,
        user=ds_config.DB_USER,
        password=ds_config.DB_PASSWORD,
        client_encoding=ds_config.DB_CLIENT_ENCODING,
        sslmode=ds_config.DB_SSL_MODE
    )

def execute(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        connection.commit()
    return result

def execute_no_fetch(connection, sql):
    rowcount = 0
    with connection.cursor() as cursor:
        cursor.execute(sql)
        rowcount = cursor.rowcount
        connection.commit()
    return rowcount

def gen_process_instance_id(connection):
    result = execute(connection, 'select nextval(\'t_ds_process_instance_id_sequence\')')
    return result[0][0]

def gen_task_instance_id(connection):
    result = execute(connection, 'select nextval(\'t_ds_task_instance_id_sequence\')')
    return result[0][0]

def insert_to_table(connection, table_name: str, value_maps: dict):
    fields = []
    values = []
    for k, v in value_maps.items():
        fields.append('{}'.format(k))
        values.append('null' if v is None else '\'{}\''.format(v))
    
    sql = 'insert into {} ({}) values ({})'.format(table_name, ','.join(fields), ','.join(values))
    
    return execute_no_fetch(connection, sql)


# if __name__ == '__main__':
#     conn = get_db_connection()
#     gen_process_instance_id(conn)
#     conn.close()