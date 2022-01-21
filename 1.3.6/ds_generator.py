#!/usr/bin/python3
# -*- coding: utf-8 -*-

import ds_db
import json
from datetime import datetime
import ds_config

DB_INFO = {
    'project_name': ds_config.PROJECT_NAME,
    'tenant_name': ds_config.TENANT_NAME,
    'user_name': ds_config.USER_NAME
}

PROCESS_DEFINITION_ID = [None]


def get_db_info():
    with ds_db.db_connect() as dbc:
        result = ds_db.db_execute(dbc, 'select id from t_ds_project where name = \'{}\''.format(
            DB_INFO['project_name']))
        DB_INFO['project_id'] = result[0][0]

        result = ds_db.db_execute(dbc, 'select id from t_ds_tenant where tenant_name = \'{}\''.format(
            DB_INFO['tenant_name']))
        DB_INFO['tenant_id'] = result[0][0]

        result = ds_db.db_execute(dbc, 'select id from t_ds_user where user_name = \'{}\''.format(
            DB_INFO['user_name']))
        DB_INFO['user_id'] = result[0][0]

        result = ds_db.db_execute(dbc, 'select id, name, project_id from t_ds_process_definition where project_id = {}'.format(
            DB_INFO['project_id']))
        process_ids = {row[0]: row[1] for row in result}
        process_names = {row[1]: row[0] for row in result}
        DB_INFO['process_ids'] = process_ids
        DB_INFO['process_names'] = process_names

        DB_INFO['max_process_id'] = ds_db.db_execute(
            dbc, 'select max(id) from t_ds_process_definition')[0][0]
        if DB_INFO['max_process_id'] is None:
            DB_INFO['max_process_id'] = 0

        PROCESS_DEFINITION_ID[0] = int(DB_INFO['max_process_id']) + 1


def get_exist_process_id(name):
    return DB_INFO['process_names'].get(name, -1)


TASK_ID = [0]


def gen_task_id():
    n = 'task-{:0>6d}'.format(TASK_ID[0])
    TASK_ID[0] += 1
    return n


def add_node_to_task_dict(task_dict: dict, task_node: dict):
    task_dict[task_node["name"]] = task_node


def gen_shell_task_node(
    task_name: str,
    script: str,
    pre_task_names: list[str],
    description: str,
    priority: str,
    worker_group: str = 'default'
) -> dict:
    node = {
        "conditionResult": {
            "successNode": [
                ""
            ],
            "failedNode": [
                ""
            ]
        },
        "description": description,
        "runFlag": "NORMAL",
        "type": "SHELL",
        "params": {
            "rawScript": script,
            "localParams": [
                {
                    "prop": "bizdate",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "${global_bizdate}"
                }
            ],
            "resourceList": []
        },
        "timeout": {
            "enable": False,
            "strategy": ""
        },
        "maxRetryTimes": "0",
        "taskInstancePriority": priority,
        "name": task_name,
        "dependence": {},
        "retryInterval": "1",
        "preTasks": pre_task_names,
        "id": gen_task_id(),
        "workerGroup": worker_group
    }

    return node


def gen_sub_process_task_node(
    task_name: str,
    pre_task_names: list[str],
    description: str,
    priority: str,
    worker_group: str = 'default'
) -> dict:
    node = {
        "taskInstancePriority": priority,
        "conditionResult": {
            "successNode": [
                ""
            ],
            "failedNode": [
                ""
            ]
        },
        "name": task_name,
        "description": description,
        "dependence": {},
        "preTasks": pre_task_names,
        "id": gen_task_id(),
        "runFlag": "NORMAL",
        "workerGroup": worker_group,
        "type": "SUB_PROCESS",
        "params": {
            "processDefinitionId": get_exist_process_id(task_name),
            "foobar_processDefinitionName": task_name
        },
        "timeout": {
            "enable": False,
            "strategy": ""
        }
    }

    return node


def gen_denpendent_task_node(
    task_name: str,
    pre_task_names: list[str],
    dep_definition_name: str,
    dep_task_name: str,
    cycle: str,
    dep_date_value: str,
    description: str,
    priority: str,
    worker_group: str = 'default'
) -> dict:
    node = {
        "conditionResult": {
            "successNode": [
                ""
            ],
            "failedNode": [
                ""
            ]
        },
        "description": description,
        "runFlag": "NORMAL",
        "type": "DEPENDENT",
        "params": {},
        "timeout": {
            "enable": False,
            "strategy": ""
        },
        "maxRetryTimes": "0",
        "taskInstancePriority": priority,
        "name": task_name,
        "dependence": {
            "dependTaskList": [
                {
                    "dependItemList": [
                        {
                            "dateValue": dep_date_value,
                            "definitionName": dep_definition_name,
                            "depTasks": dep_task_name,
                            "projectName": DB_INFO['project_name'],
                            "projectId": DB_INFO['project_id'],
                            "cycle": cycle,
                            "definitionId": get_exist_process_id(dep_definition_name)
                        }
                    ],
                    "relation": "AND"
                }
            ],
            "relation": "AND"
        },
        "retryInterval": "1",
        "preTasks": pre_task_names,
        "id": gen_task_id(),
        "workerGroup": worker_group
    }

    return node


GLOBAL_PARAMS = [
    {
        "prop": "global_bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${system.biz.date}"
    }
]


def gen_process_node(task_dict: dict) -> dict:
    node = {
        "tenantId": 1,
        "globalParams": GLOBAL_PARAMS,
        "tasks": [task_node for task_node in task_dict.values()],
        "timeout": 0
    }

    return node


def gen_connections(task_dict: dict) -> list[dict]:
    connections = []
    for task_node in task_dict.values():
        for pre_task_node_name in task_node["preTasks"]:
            connections.append(
                {
                    "endPointSourceId": task_dict[pre_task_node_name]["id"],
                    "endPointTargetId": task_node["id"]
                }
            )
    return connections


class DagNode:
    def __init__(self):
        self.prev: set[DagNode] = set()
        self.next: set[DagNode] = set()
        self.optimize_prev: set[DagNode] = set()
        self.name: str = None
        self.cleared = False
        self.count_prev: int = 0
        self.count_next: int = 0
        self.dag_level = 0


def dag_node_sort_key(node: DagNode):
    # return node.name
    return (node.count_prev) + (node.count_next)


def dag_node_group_sort_key(nodes: dict[str, DagNode]):
    return len(nodes)


def clear_connected_nodes(node: DagNode) -> dict[str, DagNode]:
    if node.cleared:
        return {}

    nodes = {}
    nodes[node.name] = node
    node.cleared = True
    for pnode in node.prev:
        pnodes = clear_connected_nodes(pnode)
        nodes.update(pnodes)

    for nnode in node.next:
        nnodes = clear_connected_nodes(nnode)
        nodes.update(nnodes)

    return nodes

# a---c 跨级
# a-b-c 通过其它路径仍能搜索到


def find_redundant_chain(base_node: DagNode, check_node: DagNode) -> list[DagNode]:

    for next_node in base_node.next.copy():
        if next_node.dag_level + 1 == check_node.dag_level and check_node in next_node.next:
            return [next_node, check_node]
        result = find_redundant_chain(next_node, check_node)
        if (result):
            result.insert(0, next_node)
            return result

    return None


def optimize_chain(base_node: DagNode, task_dict: dict):
    for next_node in base_node.next.copy():
        if next_node.dag_level > base_node.dag_level + 1:
            result = find_redundant_chain(base_node, next_node)
            if (result):
                result.insert(0, base_node)
                task_dict[next_node.name]["preTasks"].remove(base_node.name)
                base_node.next.remove(next_node)
                next_node.prev.remove(base_node)
                print(
                    'removed redundant chain [{}]->[{}] by:'.format(base_node.name, next_node.name))
                print('    {}'.format(
                    '->'.join(['[{}]'.format(n.name) for n in result])
                ))


def gen_locations(task_dict: dict) -> dict:

    locations = {}
    seq = 1

    nodes: dict[str, DagNode] = {}
    for task_node in task_dict.values():
        dag_node = nodes.get(task_node["name"])
        if not dag_node:
            dag_node = DagNode()
            nodes[task_node["name"]] = dag_node
        dag_node.name = task_node["name"]
        for pre_task_name in task_node["preTasks"]:
            pre_node = nodes.get(pre_task_name)
            if not pre_node:
                pre_node = DagNode()
                nodes[pre_task_name] = pre_node
            dag_node.prev.add(pre_node)
            pre_node.next.add(dag_node)

    for node in nodes.values():
        node.count_next = len(node.next)
        node.optimize_next = node.next.copy()
        node.count_prev = len(node.prev)
        node.optimize_prev = node.prev.copy()

    # 节点分组
    groups: list[dict[str, DagNode]] = []
    while True:
        for node in nodes.values():
            if not node.cleared:
                group = clear_connected_nodes(node)
                groups.append(group)
                break
        else:
            break

    # 关系节点越多的组越靠上
    groups.sort(key=dag_node_group_sort_key, reverse=True)

    begin_yoff = 0

    for group in groups:

        xoff = 0
        yoff = begin_yoff
        max_hight = 0

        # DAG拓扑排序
        dag_level = 0

        group_level_list = group.copy()

        node_in_levels: list[list[DagNode]] = []

        # 生成DAG层级
        while len(group_level_list) > 0:
            node_in_level: list[DagNode] = []
            no_prev_list = [e for e in group_level_list.values()
                            if not e.optimize_prev]
            for dag_node in no_prev_list:
                dag_node.dag_level = dag_level
                for next_node in dag_node.next:
                    next_node.optimize_prev.remove(dag_node)
                group_level_list.pop(dag_node.name)
                node_in_level.append(dag_node)

            node_in_levels.append(node_in_level)
            dag_level += 1

        # 优化跨层级节点
        for node_in_level in node_in_levels:
            for dag_node in node_in_level:
                optimize_chain(dag_node, task_dict)

        # 生成locations
        for node_in_level in node_in_levels:
            node_in_level.sort(key=dag_node_sort_key, reverse=True)

            for dag_node in node_in_level:

                location = {
                    "name": dag_node.name,
                    "targetarr": ",".join([task_dict[prev_node.name]["id"] for prev_node in dag_node.prev]),
                    "nodenumber": str(seq),
                    "x": 200 * xoff + 100,
                    "y": 100 * yoff + 100
                }

                locations[task_dict[dag_node.name]["id"]] = location

                seq += 1
                yoff += 1

            if max_hight < yoff:
                max_hight = yoff

            xoff += 1
            yoff = begin_yoff

        begin_yoff = max_hight

    return locations


def gen_process_definition_id():
    n = PROCESS_DEFINITION_ID[0]
    PROCESS_DEFINITION_ID[0] += 1
    return n

# 序号  字段                      类型           描述
# 1    id                        int(11)        主键
# 2    name                      varchar(255)   流程定义名称
# 3    version                   int(11)        流程定义版本
# 4    release_state             tinyint(4)     流程定义的发布状态：0 未上线 , 1已上线
# 5    project_id                int(11)        项目id
# 6    user_id                   int(11)        流程定义所属用户id
# 7    process_definition_json   longtext       流程定义JSON
# 8    description               text           流程定义描述
# 9    global_params             text           全局参数
# 10   flag                      tinyint(4)     流程是否可用：0 不可用，1 可用
# 11   locations                 text           节点坐标信息
# 12   connects                  text           节点连线信息
# 13   receivers                 text           收件人
# 14   receivers_cc              text           抄送人
# 15   create_time               datetime       创建时间
# 16   timeout                   int(11)        超时时间
# 17   tenant_id                 int(11)        租户id
# 18   update_time               datetime       更新时间
# 19   modify_by                 varchar(36)    修改用户
# 20   resource_ids              varchar(255)   资源ids


def gen_process_definition(
    id: int,               # =gen_process_definition_id()
    name: str,
    version: int,
    release_state: int,
    project_id: int,
    user_id: int,
    process_node: dict,    # =gen_process_node(task_dict)
    description: str,
    global_params: dict,   # =GLOBAL_PARAMS
    flag: int,             # =1
    locations: dict,
    connects: list[dict],
    receivers: str,
    receivers_cc: str,
    create_time: datetime,
    timeout: int,
    tenant_id: int,
    update_time: datetime,
    modify_by: str,
    resource_ids: str,
):
    process_definition = {
        "id": id,
        "name": name,
        "version": version,
        "release_state": release_state,
        "project_id": project_id,
        "user_id": user_id,
        "process_definition": process_node,
        "description": description,
        "global_params": global_params,
        "flag": flag,
        "locations": locations,
        "connects": connects,
        "receivers": receivers,
        "receivers_cc": receivers_cc,
        "create_time": create_time,
        "timeout": timeout,
        "tenant_id": tenant_id,
        "update_time": update_time,
        "modify_by": modify_by,
        "resource_ids": resource_ids,
    }

    return process_definition


def refresh_process_definition_reference(process_definitions: dict):
    for process_definition in process_definitions.values():
        for task_node in process_definition["process_definition"]["tasks"]:
            if task_node["type"] == "SUB_PROCESS":
                task_node["params"]["processDefinitionId"] = process_definitions[task_node["params"]
                                                                                 ["foobar_processDefinitionName"]]["id"]
                task_node["params"].pop("foobar_processDefinitionName")
            elif task_node["type"] == "DEPENDENT":
                for depend_task in task_node["dependence"]["dependTaskList"]:
                    for depend_item in depend_task["dependItemList"]:
                        depend_item["definitionId"] = process_definitions[depend_item["definitionName"]]["id"]


def generate_process_definition_json(process_definitions: dict):
    for process_definition in process_definitions.values():
        process_definition['process_definition'] = json.dumps(
            process_definition['process_definition'])
        process_definition['global_params'] = json.dumps(
            process_definition['global_params'])
        process_definition['locations'] = json.dumps(
            process_definition['locations'])
        process_definition['connects'] = json.dumps(
            process_definition['connects'])


def merge_process_definitions_to_db(process_definitions: dict):
    pd_id = {}

    for process_definition in process_definitions.values():
        pd_id[process_definition['id']] = process_definition

    # debug output : max concurrent thread's count
    # def calc_id_count(id, ids):
    #     process_definition = pd_id[id]
    #     ids.append(id)
    #     for task_node in process_definition["process_definition"]["tasks"]:
    #         if task_node["type"] == "SUB_PROCESS":
    #             calc_id_count(task_node["params"]["processDefinitionId"], ids)

    # for process_definition in process_definitions.values():
    #     if process_definition['name'].startswith('TRIGGER'):
    #         ids = []
    #         calc_id_count(process_definition['id'], ids)
    #         print('thread count [{}]:{}'.format(
    #             process_definition['name'], len(ids) + 1))

    old_ids = list(DB_INFO['process_ids'].keys())
    for id in pd_id.keys():
        try:
            old_ids.remove(id)
        except ValueError:
            print('insert id:{} name:{}'.format(id, pd_id[id]['name']))

    with ds_db.db_connect() as dbc:
        cursor = dbc.cursor()

        if old_ids:
            for id in old_ids:
                print('delete id:{} name:{}'.format(
                    id, DB_INFO['process_ids'][id]))
            sql = 'delete from t_ds_process_definition where project_id = {} and id in ({})'.format(
                DB_INFO['project_id'],
                ','.join([str(id) for id in old_ids])
            )
            cursor.execute(sql)
            print(sql)

        # 写入新增的数据
        process_definitions_to_insert = [
            pd for pd in process_definitions.values() if pd['id'] > DB_INFO['max_process_id']
        ]

        if process_definitions_to_insert:
            cursor.executemany(
                '''insert into t_ds_process_definition values(
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                    )''',
                [
                    [
                        pd["id"],
                        pd["name"],
                        pd["version"],
                        pd["release_state"],
                        pd["project_id"],
                        pd["user_id"],
                        pd["process_definition"],
                        pd["description"],
                        pd["global_params"],
                        pd["flag"],
                        pd["locations"],
                        pd["connects"],
                        pd["receivers"],
                        pd["receivers_cc"],
                        pd["create_time"],
                        pd["timeout"],
                        pd["tenant_id"],
                        pd["update_time"],
                        pd["modify_by"],
                        pd["resource_ids"]
                    ]
                    for pd in process_definitions_to_insert
                ]
            )

        # 更新原有的数据
        process_definitions_to_update = [
            pd for pd in process_definitions.values() if pd['id'] <= DB_INFO['max_process_id']
        ]

        if process_definitions_to_update:
            cursor.executemany(
                '''update t_ds_process_definition set
                    name = %s,
                    version = %s,
                    release_state = %s,
                    project_id = %s,
                    user_id = %s,
                    process_definition_json = %s,
                    description = %s,
                    global_params = %s,
                    flag = %s,
                    locations = %s,
                    connects = %s,
                    receivers = %s,
                    receivers_cc = %s,
                    create_time = %s,
                    timeout = %s,
                    tenant_id = %s,
                    update_time = %s,
                    modify_by = %s,
                    resource_ids = %s
                    
                    where id = %s
                ''',
                [
                    [
                        pd["name"],
                        pd["version"],
                        pd["release_state"],
                        pd["project_id"],
                        pd["user_id"],
                        pd["process_definition"],
                        pd["description"],
                        pd["global_params"],
                        pd["flag"],
                        pd["locations"],
                        pd["connects"],
                        pd["receivers"],
                        pd["receivers_cc"],
                        pd["create_time"],
                        pd["timeout"],
                        pd["tenant_id"],
                        pd["update_time"],
                        pd["modify_by"],
                        pd["resource_ids"],
                        pd["id"],
                    ]
                    for pd in process_definitions_to_update
                ]
            )

        ds_db.db_update_max_id(dbc, 't_ds_process_definition')

        dbc.commit()
