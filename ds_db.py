import pymysql.cursors
import pymysql

import psycopg2



# def db_connect():
#     return pymysql.connect(host='172.16.97.122',
#                             user='dolphinscheduler',
#                             password='dolphinscheduler',
#                             database='dolphinscheduler',
#                             charset='utf8mb4',
#                             port=3306,
#                             cursorclass=pymysql.cursors.Cursor)


def db_connect():
    return psycopg2.connect(
                database='ds',
                host='127.0.0.1',
                port=5432,
                user='ds',
                password='ds',
                client_encoding='utf-8',
                sslmode='disable')


def db_execute(dbc, sql):
    result = None
    with dbc.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return result

def db_update_max_id(dbc, table_name):
    
    cursor = dbc.cursor()
    # pg
    cursor.execute(
        'select setval(\'{}_id_sequence\', (select max(id) from {}) + 1)'.format(table_name,table_name))
    
    # mysql
    # max_id = db_execute(
    #     dbc, 'select max(id) from {}'.format(table_name))[0][0]
    # cursor.execute(
    #     ' alter table {} auto_increment = {}'.format(table_name, max_id + 1))
    # cursor.close()