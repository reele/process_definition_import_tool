#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pymysql.cursors
import pymysql
import psycopg2
import ds_config


def db_connect():
    if ds_config.DB_TYPE == 'mysql':
        return pymysql.connect(host=ds_config.DB_IP,
                               user=ds_config.DB_USER,
                               password=ds_config.DB_PASSWORD,
                               database=ds_config.DB_NAME,
                               charset='utf8mb4',
                               port=ds_config.DB_PORT,
                               cursorclass=pymysql.cursors.Cursor)
    elif ds_config.DB_TYPE == 'postgresql':
        return psycopg2.connect(
            database=ds_config.DB_NAME,
            host=ds_config.DB_IP,
            port=ds_config.DB_PORT,
            user=ds_config.DB_USER,
            password=ds_config.DB_PASSWORD,
            client_encoding='utf-8',
            sslmode='disable')
    else:
        raise Exception('ds_config.DB_TYPE error')


def db_execute(dbc, sql):
    result = None
    with dbc.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
    return result


def db_update_max_id(dbc, table_name):
    with dbc.cursor() as cursor:
        if ds_config.DB_TYPE == 'mysql':
            max_id = db_execute(
                dbc, 'select max(id) from {}'.format(table_name))[0][0]
            cursor.execute(
                ' alter table {} auto_increment = {}'.format(table_name, max_id + 1))
        elif ds_config.DB_TYPE == 'postgresql':
            cursor.execute(
                'select setval(\'{}_id_sequence\', (select max(id) from {}) + 1)'.format(table_name, table_name))
