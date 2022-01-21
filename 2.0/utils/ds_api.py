#!/usr/bin/python3

import requests
import ds_config
import json


def http_prepare(path: str):
    url = ds_config.SERVER_EP + path
    header = {'token': ds_config.TOKEN}
    return (url, header)


def http_get_response_data(resp: requests.Response):

    if not resp.ok:
        raise requests.exceptions.HTTPError(resp.reason)

    resp_obj = resp.json()

    if resp_obj['code'] != 0 or resp_obj['msg'] != 'success':
        raise requests.exceptions.HTTPError(resp_obj)

    return resp_obj['data']


def get_tenant_list():
    url, header = http_prepare('/dolphinscheduler/tenants/list')

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)


def get_project_code_by_name(project_name: str):
    url, header = http_prepare('/dolphinscheduler/projects/list')

    resp = requests.get(url, headers=header)

    data = http_get_response_data(resp)

    for row in data:
        if row['name'] == project_name:
            return row['code']

    return 0


def get_process_definitions_by_project_code(project_code: int):
    path = '/dolphinscheduler/projects/{projectCode}/process-definition/list'.format(
        projectCode=project_code)

    url, header = http_prepare(path)

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)


def create_empty_process_definition(project_code: int, process_name: str, tenant_name: str):

    path = '/dolphinscheduler/projects/{projectCode}/process-definition/empty'.format(
        projectCode=project_code)

    url, header = http_prepare(path)

    data = {
        'name': process_name,
        'projectCode': project_code,
        'tenantCode': tenant_name
    }

    resp = requests.post(url, headers=header, data=data)

    return http_get_response_data(resp)


def update_process_state_by_code(project_code: int, process_code: str, process_name: str, state: str):

    path = '/dolphinscheduler/projects/{projectCode}/process-definition/{process_code}/release'.format(
        projectCode=project_code, process_code=process_code)

    url, header = http_prepare(path)

    data = {
        'code': process_code,
        'name': process_name,
        'projectCode': project_code,
        'releaseState': state
    }

    resp = requests.post(url, headers=header, data=data)

    return http_get_response_data(resp)


def update_process_definition_by_code(project_code: int, process_code: str, data: dict, is_force_update: bool):

    if is_force_update:
        update_process_state_by_code(
            project_code, process_code, data['name'], 'OFFLINE')

    path = '/dolphinscheduler/projects/{projectCode}/process-definition/{process_code}'.format(
        projectCode=project_code, process_code=process_code)

    url, header = http_prepare(path)

    resp = requests.put(url, headers=header, data=data)

    return http_get_response_data(resp)


def generate_task_codes(count: int):

    BATCH_COUNT = 100

    data = []

    while count > 0:

        path = '/dolphinscheduler/projects/{projectCode}/task-definition/gen-task-codes'

        url, header = http_prepare(path)

        params = {
            'genNum': BATCH_COUNT if count > BATCH_COUNT else count
        }

        resp = requests.get(url, headers=header, params=params)

        data += http_get_response_data(resp)

        count -= BATCH_COUNT

    return data


def get_process_simple_list_by_project_code(project_code: int):

    path = '/dolphinscheduler/projects/{projectCode}/process-definition/simple-list'.format(
        projectCode=project_code)

    url, header = http_prepare(path)

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)


def delete_process_by_code(project_code: int, process_code: int, is_force_delete: bool = False, process_name: str = ''):
    if is_force_delete:
        if not process_name:
            ValueError('if is_force_delete, process_name must have a value')
        update_process_state_by_code(
            project_code, process_code, process_name, 'OFFLINE')

    path = '/dolphinscheduler/projects/{projectCode}/process-definition/{process_code}'.format(
        projectCode=project_code, process_code=process_code)

    url, header = http_prepare(path)

    resp = requests.delete(url, headers=header)

    return http_get_response_data(resp)


def delete_process_by_codes(project_code: int, process_codes: list[int]):
    path = '/dolphinscheduler/projects/{projectCode}/process-definition/batch-delete'.format(
        projectCode=project_code)

    url, header = http_prepare(path)

    resp = requests.post(url, headers=header, data={'codes': process_codes})

    return http_get_response_data(resp)


def create_project(project_name: str, project_description: str):

    url, header = http_prepare('/dolphinscheduler/projects')

    resp = requests.post(url, headers=header, data={
                         'projectName': project_name, 'description': project_description})

    return http_get_response_data(resp)


if __name__ == '__main__':
    code = get_project_code_by_name('etl')
    with open('dump.json', 'w') as f:
        data = get_process_definitions_by_project_code(code)
        json.dump(data, f)
    exit(0)
    for i in range(10):
        create_empty_process_definition(code, 'p'+str(i), 'etl')
    codes = [e['code'] for e in get_process_simple_list_by_project_code(code)]
    delete_process_by_codes(code, codes)
