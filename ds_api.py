#!/usr/bin/python3

import requests
import ds_config
import json
from datetime import date, datetime


def http_prepare(path: str):
    url = ds_config.SERVER_EP + path
    header = {"token": ds_config.TOKEN}
    return (url, header)


def http_get_response_data(resp: requests.Response):

    if not resp.ok:
        raise requests.exceptions.HTTPError(resp.reason)

    resp_obj = resp.json()

    if resp_obj["code"] != 0 or resp_obj["msg"] != "success":
        raise requests.exceptions.HTTPError(resp_obj)

    return resp_obj["data"]


def get_tenant_list():
    url, header = http_prepare("/dolphinscheduler/tenants/list")

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)


def get_project_code_by_name(project_name: str):
    url, header = http_prepare("/dolphinscheduler/projects/list")

    resp = requests.get(url, headers=header)

    data = http_get_response_data(resp)

    for row in data:
        if row["name"] == project_name:
            return row["code"]

    return 0


def get_process_definition_by_name(project_code: int, process_name: str):
    path = "/dolphinscheduler/projects/{projectCode}/workflow-definition/query-by-name".format(
        projectCode=project_code
    )
    url, header = http_prepare(path)

    params = {"name": process_name}

    resp = requests.get(url, headers=header, params=params)

    return http_get_response_data(resp)


def get_process_definitions_by_project_code(project_code: int):
    path = "/dolphinscheduler/projects/{projectCode}/workflow-definition/list".format(
        projectCode=project_code
    )

    url, header = http_prepare(path)

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)


def get_schedules_by_project_code(project_code: int):
    path = f"/dolphinscheduler/projects/{project_code}/schedules/list"

    url, header = http_prepare(path)

    resp = requests.post(url, headers=header)

    return http_get_response_data(resp)


def create_empty_process_definition(
    project_code: int, process_name: str, tenant_name: str
):

    task_codes = generate_task_codes(1)

    fake_task_code = task_codes[0]

    path = "/dolphinscheduler/projects/{projectCode}/workflow-definition".format(
        projectCode=project_code
    )

    url, header = http_prepare(path)

    locations = [{"taskCode": fake_task_code, "x": 0, "y": 0}]
    task_definition_json = [
        {
            "code": fake_task_code,
            "delayTime": "0",
            "description": "",
            "environmentCode": -1,
            "failRetryInterval": "1",
            "failRetryTimes": "0",
            "flag": "YES",
            "name": "asd",
            "taskParams": {"localParams": [], "rawScript": "ls /-", "resourceList": []},
            "taskPriority": "MEDIUM",
            "taskType": "SHELL",
            "timeout": 0,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": "",
            "workerGroup": "default",
            "cpuQuota": -1,
            "memoryMax": -1,
            "taskExecuteType": "BATCH",
        }
    ]
    task_relation_json = [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": fake_task_code,
            "postTaskVersion": 0,
            "conditionType": "NONE",
            "conditionParams": {},
        }
    ]

    data = {
        "name": process_name,
        "projectCode": project_code,
        "tenantCode": tenant_name,
        "locations ": json.dumps(locations),
        "taskRelationJson": json.dumps(task_relation_json),
        "taskDefinitionJson": json.dumps(task_definition_json),
        "executionType": "PARALLEL",
    }

    resp = requests.post(url, headers=header, data=data)

    return http_get_response_data(resp)


def update_process_state_by_code(
    project_code: int, process_code: str, process_name: str, state: str
):

    path = "/dolphinscheduler/projects/{projectCode}/workflow-definition/{process_code}/release".format(
        projectCode=project_code, process_code=process_code
    )

    url, header = http_prepare(path)

    data = {
        "code": process_code,
        "name": process_name,
        "projectCode": project_code,
        "releaseState": state,
    }

    resp = requests.post(url, headers=header, data=data)

    return http_get_response_data(resp)


def online_schedule(project_code: int, schedule_id: int):

    path = f"/dolphinscheduler/projects/{project_code}/schedules/{schedule_id}/online"

    url, header = http_prepare(path)

    resp = requests.post(url, headers=header)

    return http_get_response_data(resp)


def update_process_definition_by_code(
    project_code: int, process_code: str, data: dict, is_force_update: bool
):

    if is_force_update:
        update_process_state_by_code(
            project_code, process_code, data["name"], "OFFLINE"
        )

    path = "/dolphinscheduler/projects/{projectCode}/workflow-definition/{process_code}".format(
        projectCode=project_code, process_code=process_code
    )

    url, header = http_prepare(path)

    resp = requests.put(url, headers=header, data=data)

    return http_get_response_data(resp)


def generate_task_codes(count: int):

    BATCH_COUNT = 100

    data = []

    while count > 0:

        path = "/dolphinscheduler/projects/{projectCode}/task-definition/gen-task-codes"

        url, header = http_prepare(path)

        params = {"genNum": BATCH_COUNT if count > BATCH_COUNT else count}

        resp = requests.get(url, headers=header, params=params)

        data += http_get_response_data(resp)

        count -= BATCH_COUNT

    return data


def get_process_simple_list_by_project_code(project_code: int):

    path = "/dolphinscheduler/projects/{projectCode}/workflow-definition/simple-list".format(
        projectCode=project_code
    )

    url, header = http_prepare(path)

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)


def delete_process_by_code(
    project_code: int,
    process_code: int,
    is_force_delete: bool = False,
    process_name: str = "",
):
    if is_force_delete:
        if not process_name:
            ValueError("if is_force_delete, process_name must have a value")
        update_process_state_by_code(
            project_code, process_code, process_name, "OFFLINE"
        )

    path = "/dolphinscheduler/projects/{projectCode}/workflow-definition/{process_code}".format(
        projectCode=project_code, process_code=process_code
    )

    url, header = http_prepare(path)

    resp = requests.delete(url, headers=header)

    return http_get_response_data(resp)


def delete_process_by_codes(project_code: int, process_codes: list):
    path = "/dolphinscheduler/projects/{projectCode}/workflow-definition/batch-delete".format(
        projectCode=project_code
    )

    url, header = http_prepare(path)

    resp = requests.post(url, headers=header, data={"codes": process_codes})

    return http_get_response_data(resp)


def get_environment_code_by_name(project_name: str):
    url, header = http_prepare("/dolphinscheduler/environment/query-environment-list")

    resp = requests.get(url, headers=header)

    data = http_get_response_data(resp)

    for row in data:
        if row["name"] == project_name:
            return row["code"]

    raise ValueError()


def create_project(project_name: str, project_description: str):

    url, header = http_prepare("/dolphinscheduler/projects")

    resp = requests.post(
        url,
        headers=header,
        data={"projectName": project_name, "description": project_description},
    )

    return http_get_response_data(resp)


def get_top_process_instance(project_code: int, start_date: date, end_date: date):

    path = f"/dolphinscheduler/projects/{project_code}/process-instances"

    url, header = http_prepare(path)

    params = {
        "pageNo": 1,
        "pageSize": 200,
        "startDate": start_date.strftime("%Y-%m-%d 00:00:00"),
        "endDate": end_date.strftime("%Y-%m-%d 00:00:00"),
    }

    resp = requests.get(url, headers=header, params=params)

    return http_get_response_data(resp)


def get_task_by_process_instance_id(project_code: int, process_instance_id: int):

    path = f"/dolphinscheduler/projects/{project_code}/process-instances/{process_instance_id}/tasks"

    url, header = http_prepare(path)

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)


def get_process_instance_id_by_task_id(project_code: int, task_id: int, task_code: int):

    path = f"/dolphinscheduler/projects/{project_code}/process-instances/query-sub-by-parent"

    url, header = http_prepare(path)

    params = {"taskCode": task_code, "taskId": task_id}

    resp = requests.get(url, headers=header, params=params)

    data = http_get_response_data(resp)

    return data["subProcessInstanceId"]


def get_environments():

    path = f"/dolphinscheduler/environment/query-environment-list"

    url, header = http_prepare(path)

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)


def get_worker_groups():

    path = f"/dolphinscheduler/worker-groups/all"

    url, header = http_prepare(path)

    resp = requests.get(url, headers=header)

    return http_get_response_data(resp)

def start_fake_workflow(project_name, workflow_name, schedule_time: datetime, tenant_name):

    project_code = get_project_code_by_name(project_name)

    workflow_definition = get_process_definition_by_name(project_code, workflow_name)

    workflow_obj: dict = workflow_definition["workflowDefinition"]
    workflow_code = workflow_obj["code"]

    
    path = f"/dolphinscheduler/projects/{project_code}/executors/start-workflow-instance"

    url, header = http_prepare(path)

    complement_dates_json = {"complementScheduleDateList":"{}".format(schedule_time.strftime("%Y-%m-%d %H:%M:%S"))}

    # workflowDefinitionCode: 131589853968448
    # failureStrategy: CONTINUE
    # warningType: NONE
    # warningGroupId: 
    # execType: COMPLEMENT_DATA
    # startNodeList: 
    # taskDependType: TASK_POST
    # complementDependentMode: OFF_MODE
    # runMode: RUN_MODE_SERIAL
    # workflowInstancePriority: MEDIUM
    # workerGroup: default
    # tenantCode: default
    # environmentCode: 
    # startParams: 
    # expectedParallelismNumber: 2
    # dryRun: 0
    # testFlag: 0
    # version: 1
    # allLevelDependent: false
    # executionOrder: DESC_ORDER
    # scheduleTime: {"complementScheduleDateList":"2025-01-02 00:00:00,2025-01-03 00:00:00"}

    data = {
        "workflowDefinitionCode": workflow_code,
        "scheduleTime": json.dumps(complement_dates_json),
        "execType": "COMPLEMENT_DATA",
        "workflowInstancePriority": "MEDIUM",
        "dryRun": 1,
        "failureStrategy": "CONTINUE",
        "warningType": "NONE",
        "expectedParallelismNumber": 1,
        "complementDependentMode": "OFF_MODE",
        "runMode": "RUN_MODE_SERIAL",
        "executionOrder": "ASC_ORDER",
        'tenantCode': tenant_name
    }

    resp = requests.post(url, headers=header, data=data)

    return http_get_response_data(resp)


if __name__ == "__main__":
    start_fake_workflow('[dw_main][1104_PCD]', 'T02_CUST_FNC_SIGN_ACCT_S04_1', datetime(2025, 1, 25))
