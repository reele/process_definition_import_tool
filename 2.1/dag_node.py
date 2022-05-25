#!/usr/bin/python3

from datetime import datetime
import json
from typing import Any
import ds_api
from simple_log import print_and_log, LOG_TIME_STR
import ds_db


SUB_PROCESS_DISABLED = [
# 'T05_OPN_BNK_FX_STL_EVE_UP_H_S09',
# 'T01_CUST_RISK_INFO_H_S04_1',
# 'T01_FUND_CUST_BASIC_INFO_S10_1',
# 'T01_PTY_CLS_H_S11_1',
# 'T01_PTY_IDTFY_H_S01_1',
# 'T01_PTY_IDTFY_H_S02_1',
# 'T01_PTY_IDTFY_H_S03_1',
# 'T01_PTY_IDTFY_H_S04_1',
# 'T01_PTY_IDTFY_H_S09_1',
# 'T01_PTY_IDTFY_H_S10_1',
# 'T01_PTY_IDTFY_H_S11_1'
]

def confirm(prompt, msg_yes, msg_no):
    while True:
        value = input(prompt)
        value = value.lower()
        if value == 'yes':
            if msg_yes:
                print(msg_yes)
            return True
        if value == 'no':
            if msg_no:
                print(msg_no)
            return False

class AutoParams:
    def __init__(self, seqs: dict[str, Any]):
        self.params = seqs

    def next(self, key):
        """从集合中取对象的下一个迭代值
        int : += 1
        list: pop()
        """
        value = self.params[key]
        if isinstance(value, int):
            self.params[key] = value + 1
            return value
        elif isinstance(value, list):
            return value.pop()
        else:
            raise TypeError()

    def get(self, key):
        """从集合中取对象的值"""
        value = self.params[key]
        return value
        # if isinstance(value, int):
        #     return value
        # elif isinstance(value, str):
        #     return value
        # elif isinstance(value, list):
        #     return value[-1]
        # else:
        #     raise TypeError()

    def pick(self, params_key, key):
        """从集合中取出对象
        dict: pop()
        """
        value = self.params[params_key]
        if isinstance(value, dict):
            return value.pop(key)
        else:
            raise TypeError()

    def reset(self, str, value):
        self.params[str] = value


def compare_dict(l: dict[str:Any], r: dict[str:Any], ignored_keys: list[str]):

    errors: dict[str, tuple] = {}

    for k, v in l.items():
        if k in ignored_keys:
            continue
        rv = r.get(k)
        
        if isinstance(v, dict):
            if rv:
                if isinstance(rv, dict):
                    sub_errors = compare_dict(v, rv, ignored_keys)
                    if sub_errors:
                        errors[k] = sub_errors
                    continue
        
        if v != rv:
            errors[k] = (v, rv)


    return errors


class DAGNode(object):
    ignored_keys_tr = [
        'id',
        'preTaskCode',
        'postTaskCode',
        'createTime',
        'updateTime',
        'processDefinitionVersion',
        'preTaskVersion',
        'postTaskVersion'
    ]

    ignored_keys_td = [
        'id',
        'version',
        'userId',
        # 'taskParams',
        'taskParamList',
        'createTime',
        'updateTime',
        'taskParamMap',
        'description',
        'taskPriority',
        'timeoutNotifyStrategy'
    ]
    
    ignored_keys_td_void = [
        'id',
        'version',
        'userId',
        'taskParams',
        'taskParamList',
        'createTime',
        'updateTime',
        'taskParamMap',
        'description',
        'taskPriority',
        # 'flag',
        'timeoutNotifyStrategy'
    ]
    
    class DAGJsonEncoder(json.JSONEncoder):
        def default_children(self, o):
            if isinstance(o, DAGNode):
                node_object = {
                    "name": o.name,
                    "type": o.type,
                    "cycle": o.cycle,
                    "path": o.path,
                    "prev_nodes": [node.name for node in o.prev_nodes],
                    "next_nodes": [node.name for node in o.next_nodes],
                }

                return node_object
            return None

        def default(self, o):
            try:
                if isinstance(o, DAGNode):
                    node_object = {
                        "name": o.name,
                        "type": o.type,
                        "cycle": o.cycle,
                        "path": o.path,
                        "prev_nodes": [node.name for node in o.prev_nodes],
                        "next_nodes": [node.name for node in o.next_nodes],
                        "children": [
                            self.default_children(child) for child in o.children
                        ],
                    }
            except TypeError:
                pass
            else:
                return node_object
            # Let the base class default method raise the TypeError
            return json.JSONEncoder.default(self, o)

    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    TASK_DEF_TYPE_MAPPING = {
        "process": "SUB_PROCESS",
        "shell": "SHELL",
        "dependent": "DEPENDENT",
        "self_dependent": "DEPENDENT",
    }

    def __init__(self, type: str = None, name: str = None):
        self.group_code: str = None
        self.type: str = type
        self.name: str = name
        self.children: list[DAGNode] = []
        self.parent: DAGNode = None
        self.dependents: set[DAGNode] = set()
        self.dependency_node: DAGNode = None
        self.dependencies: set[DAGNode] = set()

        self.max_dag_dep_level = 0

        self.command: str = None
        self.prev_nodes: list[DAGNode] = []
        self.next_nodes: list[DAGNode] = []
        self.description: str = None
        self.path: str = None
        self.cycle: str = None
        self.continuous_check_date: str = None
        self.ds_origin_task: dict = None
        self.ds_task_code: int = 0
        self.ds_origin_process: dict = None
        self.ds_process_code: int = 0
        self.ds_task_id: int = 0
        self.ds_process_id: int = 0
        self.ds_project_code: int = 0
        self.ds_node: dict = None
        self.ds_relations: list[dict] = None
        self.ds_locations: list[dict] = None
        self.ds_task: dict = None
        self.ds_tasks: list = None
        self.ds_process: dict = None
        self.ds_modify_desc: str = None
        self.ds_task_name: str = None
        self.fail_retry_times = 0
        self.fail_retry_interval = 1

        # dag optimize
        self.count_next: int = -1
        self.count_prev: int = -1
        self.optimize_next: list[DAGNode] = None
        self.optimize_prev: list[DAGNode] = None
        self.cleared = False
        self.dag_level = 0

    def reset_optimize_data(self):
        self.count_next = len(self.next_nodes)
        self.optimize_next = self.next_nodes.copy()
        self.count_prev = len(self.prev_nodes)
        self.optimize_prev = self.prev_nodes.copy()
        self.cleared = False
        self.dag_level = 0

    # def get_task_count(self):
    #     if self.type == 'root':
    #         count = 0
    #         for child_node in self.children:
    #             count += child_node.get_task_count()
    #         return count
    #     elif self.type == 'process':
    #         count = 1
    #         if self.children:
    #             print('{} child count: {}'.format(
    #                 self.name, len(self.children)))
    #         for child_node in self.children:
    #             count += child_node.get_task_count()
    #         return count
    #     else:
    #         return 1

    def create_processes_and_codes(self, params: AutoParams):
        self.ds_project_code = params.get("project_code")

        if self.parent.type == "process":
            # 是流程下的子任务, 生成任务编号
            task_idendifier = "{process_name}:{task_name}:{task_type}".format(
                process_name=self.parent.name,
                task_name=self.name,
                task_type=DAGNode.TASK_DEF_TYPE_MAPPING[self.type],
            )

            if self.ds_task_code == 0:
                try:
                    code = params.next("task_codes")
                except IndexError:
                    params.reset("task_codes", ds_api.generate_task_codes(200))
                    code = params.next("task_codes")
                data = {"code": code}
                print_and_log(
                    "merge",
                    "建立任务代码 {}::{}.".format(task_idendifier, code),
                )
                self.ds_task_code = data["code"]
            self.ds_task_id = 0  # params.next('id')

        if self.type == "process":
            # 按名字取出已有的process, 不存在时新建
            if self.ds_process_code == 0:
                data = ds_api.create_empty_process_definition(
                    self.ds_project_code, self.name, params.get("tenant_name")
                )
                self.ds_process_code = data["code"]

                print_and_log(
                    "merge",
                    "建立空流程 {} code:{} id:{}.".format(
                        self.name, self.ds_process_code, self.ds_process_id
                    ),
                )
            self.ds_process_id = 0

            for child_node in self.children:
                child_node.create_processes_and_codes(params)

    def match_processes_and_codes(self, params: AutoParams):
        self.ds_project_code = params.get("project_code")

        if self.parent.type == "process":
            # 是流程下的子任务, 生成任务编号
            task_idendifier = "{process_name}:{task_name}:{task_type}".format(
                process_name=self.parent.name,
                task_name=self.ds_task_name if self.type == 'process' and self.ds_task_name else self.name,
                task_type=DAGNode.TASK_DEF_TYPE_MAPPING[self.type],
            )
            try:
                data = params.pick("old_tasks", task_idendifier)
                self.ds_origin_task = data
            except KeyError:
                data = {"code": 0}
            self.ds_task_code = data["code"]
            self.ds_task_id = 0

        if self.type == "process":
            # 按名字取出已有的process, 不存在时新建
            try:
                data = params.pick("old_process", self.name)
                self.ds_origin_process = params.pick("origin_processes", self.name)
            except KeyError:
                data = {"code": 0}

            self.ds_process_code = data["code"]
            self.ds_process_id = 0

            for child_node in self.children:
                child_node.match_processes_and_codes(params)

    def gen_ds_task_node(self, params: AutoParams):
        flag = 'YES'
        name = self.name
        if self.type == "process":
            task_type = "SUB_PROCESS"
            task_description = self.description
            task_params = {
                "processDefinitionCode": self.ds_process_code,
                "dependence": {},
                "conditionResult": {"successNode": [], "failedNode": []},
                "waitStartTimeout": {},
                "switchResult": {},
            }
            if self.name in SUB_PROCESS_DISABLED:
                flag = 'NO'
            
            if self.ds_task_name:
                name = self.ds_task_name
        elif self.type == "shell":
            task_type = "SHELL"
            task_description = self.description
            task_params = {
                "localParams": [
                    {
                        "prop": "bizdate",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "${global_bizdate}",
                    }
                ],
                "resourceList": [],
                "rawScript": "echo '{}'".format(self.command.replace("'", "\\'"))
                if params.get("is_generate_void")
                else self.command,
                "dependence": {},
                "conditionResult": {"successNode": [], "failedNode": []},
                "waitStartTimeout": {},
                "switchResult": {},
            }
        elif self.type == "dependent" or self.type == "self_dependent":
            task_type = "DEPENDENT"
            task_description = self.description
            task_params = {
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "projectCode": self.ds_project_code,
                                    "definitionCode": self.dependency_node.ds_process_code if self.dependency_node.type == 'process' else self.dependency_node.parent.ds_process_code,
                                    "depTaskCode": 0 if self.dependency_node.type == 'process' else self.dependency_node.ds_task_code,
                                    "cycle": self.cycle,
                                    "dateValue": self.continuous_check_date,
                                }
                            ],
                        }
                    ],
                },
                "conditionResult": {"successNode": [], "failedNode": []},
                "waitStartTimeout": {
                    "strategy": "FAILED",
                    "interval": None,
                    "checkInterval": None,
                    "enable": False,
                },
                "switchResult": {},
            }

            if params.get("is_generate_void"):
                flag = 'NO'

            self.fail_retry_times = 1080
            self.fail_retry_interval = 1
        else:
            raise TypeError("Unknown type")

        task_priority = 'MEDIUM'
        # task_priority = "HIGH" if self.dependents else "MEDIUM"

        self.ds_task = {
            # "id": self.ds_task_id,
            "code": self.ds_task_code,
            "name": name,
            "version": 1,
            "description": task_description,
            "projectCode": self.ds_project_code,
            "userId": 1,
            "taskType": task_type,
            "taskParams": task_params,
            "taskParamList": [],
            "taskParamMap": {},
            "flag": flag,
            "taskPriority": task_priority,
            "userName": None,
            "projectName": None,
            "workerGroup": "default",
            "environmentCode": params.get("environment_code"),
            "failRetryTimes": self.fail_retry_times,
            "failRetryInterval": self.fail_retry_interval,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": None,
            "timeout": 0,
            "delayTime": 0,
            "resourceIds": "",
            "createTime": params.get('operation_time'),
            "updateTime": params.get('operation_time'),
            "modifyBy": None,
        }

        return self.ds_task

    def gen_task_locations(self) -> dict:

        locations = []

        for child in self.children:
            child.reset_optimize_data()

        # 节点分组
        def gen_task_locations_clear_connected_nodes(
            node: DAGNode,
        ) -> dict[str, DAGNode]:
            if node.cleared:
                return {}

            nodes = {}
            nodes[node.name] = node
            node.cleared = True
            for pnode in node.prev_nodes:
                pnodes = gen_task_locations_clear_connected_nodes(pnode)
                nodes.update(pnodes)

            for nnode in node.next_nodes:
                nnodes = gen_task_locations_clear_connected_nodes(nnode)
                nodes.update(nnodes)

            return nodes

        groups: list[dict[str, DAGNode]] = []
        while True:
            for node in self.children:
                if not node.cleared:
                    group = gen_task_locations_clear_connected_nodes(node)
                    groups.append(group)
                    break
            else:
                break

        # 关系节点越多的组越靠上
        def group_sort_key(nodes):
            return len(nodes)

        groups.sort(key=group_sort_key, reverse=True)

        begin_yoff = 0

        # 优化跨层级节点
        def find_redundant_chain(
            base_node: DAGNode, check_node: DAGNode
        ) -> list[DAGNode]:

            for next_node in base_node.next_nodes:
                if (
                    next_node.dag_level + 1 == check_node.dag_level
                    and check_node in next_node.next_nodes
                ):
                    return [next_node, check_node]
                result = find_redundant_chain(next_node, check_node)
                if result:
                    result.insert(0, next_node)
                    return result

            return None

        def optimize_chain(base_node: DAGNode):
            for next_node in base_node.next_nodes.copy():
                if next_node.dag_level > base_node.dag_level + 1:
                    result = find_redundant_chain(base_node, next_node)
                    if result:
                        result.insert(0, base_node)
                        base_node.next_nodes.remove(next_node)
                        next_node.prev_nodes.remove(base_node)
                        print_and_log(
                            "optimize",
                            "移除冗余依赖 [{}]->[{}] 自:".format(
                                base_node.name, next_node.name
                            ),
                        )
                        print_and_log(
                            "optimize",
                            "    {}".format(
                                "->".join(["[{}]".format(n.name) for n in result])
                            ),
                        )

        flat_posx = 0

        for group in groups:

            xoff = 0
            yoff = begin_yoff
            max_hight = 0

            # DAG拓扑排序
            dag_level = 0

            group_level_list = group.copy()

            node_in_levels: list[list[DAGNode]] = []

            # 生成DAG层级
            while len(group_level_list) > 0:
                node_in_level: list[DAGNode] = []
                no_prev_list = [
                    e for e in group_level_list.values() if not e.optimize_prev
                ]
                for dag_node in no_prev_list:
                    dag_node.dag_level = dag_level
                    for next_node in dag_node.optimize_next:
                        next_node.optimize_prev.remove(dag_node)
                    group_level_list.pop(dag_node.name)
                    node_in_level.append(dag_node)

                node_in_levels.append(node_in_level)
                dag_level += 1

            # 优化跨层级节点
            for node_in_level in node_in_levels:
                for dag_node in node_in_level:
                    optimize_chain(dag_node)

            # 生成locations

            def node_sort_key(node):
                return (node.count_prev) + (node.count_next)

            if len(node_in_levels) == 1 and len(node_in_levels[0]) == 1:
                dag_node = node_in_levels[0][0]
                location = {
                    "taskCode": dag_node.ds_task_code,
                    "x": 300 * flat_posx + 100,
                    "y": 100 * yoff + 100,
                }

                locations.append(location)

                flat_posx += 1
                if flat_posx >= 4:
                    flat_posx = 0
                    yoff += 1

                if max_hight < yoff:
                    max_hight = yoff

            else:
                node_offset = 0

                for node_in_level in node_in_levels:
                    node_in_level.sort(key=node_sort_key, reverse=True)

                    node_offset_reverse = len(node_in_level)

                    for dag_node in node_in_level:

                        # size = 220 * 50
                        location = {
                            "taskCode": dag_node.ds_task_code,
                            "x": 300 * xoff
                            + 100
                            + (node_offset + node_offset_reverse) * 10,
                            "y": 100 * yoff + 100,
                        }

                        locations.append(location)

                        yoff += 1
                        node_offset_reverse -= 1

                    node_offset += len(node_in_level)

                    if max_hight < yoff:
                        max_hight = yoff

                    xoff += 1
                    yoff = begin_yoff

            begin_yoff = max_hight

        return locations

    def gen_task_relations(self, params: AutoParams):
        relations = []
        for child in self.children:
            for prev_node in child.prev_nodes:
                relations.append(
                    {
                        # "id": 0,#seqs.next('id'),
                        "name": "",
                        "processDefinitionVersion": 1,
                        "projectCode": self.ds_project_code,
                        "processDefinitionCode": self.ds_process_code,
                        "preTaskCode": prev_node.ds_task_code,
                        "preTaskVersion": 1,
                        "postTaskCode": child.ds_task_code,
                        "postTaskVersion": 1,
                        "conditionType": "NONE",
                        "conditionParams": {},
                        "createTime": params.get('operation_time'),
                        "updateTime": params.get('operation_time'),
                    }
                )
            if not child.prev_nodes:
                relations.append(
                    {
                        # "id": 0, # seqs.next('id'),
                        "name": "",
                        "processDefinitionVersion": 1,
                        "projectCode": self.ds_project_code,
                        "processDefinitionCode": self.ds_process_code,
                        "preTaskCode": 0,
                        "preTaskVersion": 1,
                        "postTaskCode": child.ds_task_code,
                        "postTaskVersion": 1,
                        "conditionType": "NONE",
                        "conditionParams": {},
                        "createTime": params.get('operation_time'),
                        "updateTime": params.get('operation_time'),
                    }
                )

        return relations

    def gen_init_ds_node(self, params: AutoParams):
        
        if self != params.get('init_process'):
            raise ValueError('this method only used for init_process_node')
        
        ds_task_nodes = []
        for child_node in self.children:
            if child_node.type == "process":
                if not child_node.ds_task:
                    continue
                task_ref = child_node.ds_task.copy()
                task_ref['name'] = task_ref['name']+'_init_task'

                try:
                    code = params.next("task_codes")
                except IndexError:
                    params.reset("task_codes", ds_api.generate_task_codes(200))
                    code = params.next("task_codes")
                data = {"code": code}
                task_ref['code'] = data["code"]
                ds_task_nodes.append(task_ref)
            else:
                raise ValueError('init node has nonprocess child')
        
        relations = []
        for ds_task_node in ds_task_nodes:
            relations.append(
                {
                    # "id": 0, # seqs.next('id'),
                    "name": "",
                    "processDefinitionVersion": 1,
                    "projectCode": self.ds_project_code,
                    "processDefinitionCode": self.ds_process_code,
                    "preTaskCode": 0,
                    "preTaskVersion": 1,
                    "postTaskCode": ds_task_node['code'],
                    "postTaskVersion": 1,
                    "conditionType": "NONE",
                    "conditionParams": {},
                    "createTime": params.get('operation_time'),
                    "updateTime": params.get('operation_time'),
                }
            )

        self.ds_process = {
            "code": self.ds_process_code,
            "locations": json.dumps([]),
            "name": self.name,
            "projectCode": self.ds_project_code,
            "taskDefinitionJson": json.dumps(ds_task_nodes),
            "taskRelationJson": json.dumps(relations),
            "tenantCode": "etl",
            "description": self.description,
            "globalParams": json.dumps(
                [
                    {
                        "prop": "global_bizdate",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "${system.biz.date}",
                    }
                ]
            ),
            "releaseState": "ONLINE",
            "timeout": 0,
        }

    def gen_ds_node(self, params: AutoParams):
        if self.type == "process":
            ds_task_nodes = []
            for child_node in self.children:
                if child_node.type == "process":
                    child_node.gen_ds_node(params)
                    ds_task_nodes.append(child_node.gen_ds_task_node(params))
                else:
                    ds_task_nodes.append(child_node.gen_ds_node(params))

            self.ds_locations = self.gen_task_locations()

            self.ds_relations = self.gen_task_relations(params)

            self.ds_tasks = ds_task_nodes

            self.ds_process = {
                "code": self.ds_process_code,
                "locations": json.dumps(self.ds_locations),
                "name": self.name,
                "projectCode": self.ds_project_code,
                "taskDefinitionJson": json.dumps(self.ds_tasks),
                "taskRelationJson": json.dumps(self.ds_relations),
                "tenantCode": "etl",
                "description": self.description,
                "globalParams": json.dumps(
                    [
                        {
                            "prop": "global_bizdate",
                            "direct": "IN",
                            "type": "VARCHAR",
                            "value": "${system.biz.date}",
                        }
                    ]
                ),
                "releaseState": "ONLINE",
                "timeout": 0,
            }

            # result = ds_api.update_process_definition_by_code(
            #     self.ds_project_code, self.ds_process_code, self.ds_process, True)
            # print('writed process:{}, code:{} with {} tasks, is_generate_void:{}'.format(
            #     self.name, self.ds_process_code, len(self.children),
            #     params.get('is_generate_void')
            #     ))

        else:
            self.ds_node = self.gen_ds_task_node(params)
            return self.ds_node

    def compare_changes(self, params: AutoParams):
        if self.type == "process":
            self.ds_modify_desc = ''
            for child_node in self.children:
                if child_node.type == "process":
                    child_node.compare_changes(params)
                elif child_node.ds_modify_desc:
                    self.ds_modify_desc = 'update'

            if self.ds_origin_process:
                process_def = self.ds_origin_process['processDefinition']
                for key, value in self.ds_process.items():
                    if key == "locations":
                        continue
                    elif key == "taskRelationJson":
                        l_list = json.loads(value)
                        r_list = self.ds_origin_process["processTaskRelationList"]
                        local_relation_dict = {
                            str(l["preTaskCode"]) + "_" + str(l["postTaskCode"]): l
                            for l in l_list
                        }
                        remote_relation_dict = {
                            str(r["preTaskCode"]) + "_" + str(r["postTaskCode"]): r
                            for r in r_list
                        }

                        for relation_key, relation_value in local_relation_dict.items():
                            remote_relation_value = remote_relation_dict.get(relation_key)
                            if not remote_relation_value:
                                print_and_log(
                                    'cover_change',
                                    "将建立流程关系:[{}]::[{}] ".format(
                                        self.name, relation_key
                                    )
                                )
                                self.ds_modify_desc = 'update'
                            else:
                                sub_errors = compare_dict(relation_value, remote_relation_value, DAGNode.ignored_keys_tr)
                                if sub_errors:
                                    print_and_log(
                                        'cover_change',
                                        "将建立流程关将更新:[{}]::[{}]:({})".format(
                                            self.name, relation_key, sub_errors
                                        )
                                    )
                                    self.ds_modify_desc = 'update'
                        
                        diff_keys = set(remote_relation_dict) - set(local_relation_dict)
                        if diff_keys:
                            for sk in diff_keys:
                                print_and_log(
                                    'cover_change',
                                    "将删除流程关系:[{}]::[{}]:({})".format(
                                        self.name, sk, remote_relation_dict[sk]
                                    )
                                )
                            self.ds_modify_desc = 'update'

                    elif key == "taskDefinitionJson":
                        l_list = json.loads(value)
                        r_list = self.ds_origin_process["taskDefinitionList"]
                        local_task_dict = {l["name"]: l for l in l_list}
                        remote_task_dict = {r["name"]: r for r in r_list}

                        # if params.get("is_generate_void") and 

                        for task_key, task_value in local_task_dict.items():
                            rv = remote_task_dict.get(task_key)
                            if not rv:
                                print_and_log(
                                    'cover_change',
                                    "将建立任务:[{}]::[{}][{}]".format(
                                        self.name, task_value['taskType'], task_key
                                    )
                                )
                                self.ds_modify_desc = 'update'
                            else:
                                if params.get("is_generate_void") and task_value['taskType'] == 'DEPENDENT':
                                    sub_errors = compare_dict(task_value, rv, DAGNode.ignored_keys_td_void)
                                else:
                                    sub_errors = compare_dict(task_value, rv, DAGNode.ignored_keys_td)
                                if sub_errors:
                                    print_and_log(
                                        'cover_change',
                                        "将更新任务:[{}]::[{}][{}]:({})".format(
                                            self.name, task_value['taskType'], task_key, sub_errors
                                        )
                                    )
                                    self.ds_modify_desc = 'update'
                        
                        diff_keys = set(remote_task_dict) - set(local_task_dict)
                        if diff_keys:
                            for sk in diff_keys:
                                print_and_log(
                                    'cover_change',
                                    "将删除任务:[{}]::[{}][{}]:({})".format(
                                        self.name, task_value['taskType'], sk, remote_task_dict[sk]
                                    )
                                )
                            self.ds_modify_desc = 'update'
                    
                    elif key == 'globalParams':
                        if json.loads(value) != json.loads(process_def[key]):
                            print_and_log(
                                'cover_change',
                                "process:[{}], relation:[{}] will update:({},{})".format(
                                    self.name, relation_key, value, process_def[key]
                                )
                            )
                            self.ds_modify_desc = 'update'
                    else:
                        # temprary
                        if key in ('tenantCode', 'description'):
                            continue
                        if value != process_def[key]:
                            print_and_log(
                                'cover_change',
                                "process:[{}].[{}] will change:({}, {})".format(
                                    self.name, key, value, process_def[key]
                                )
                            )
                            self.ds_modify_desc = 'update'
                today = datetime.now().strftime("%Y-%m-%d 00:00:00")
                # if process_def['updateTime'] >= today:
                #     self.ds_modify_desc = 'update'
                if self.ds_modify_desc:
                    print_and_log(
                        'cover_change',
                        "将更新流程:[{}]".format( self.name )
                    )
                    params.next('changed_process_count')
                    
                    for child in self.children:
                        if child.type in ('self_dependent', 'dependent'):
                            # 增加新流程的初始化调用流程, 手动调用后手动删除
                            init_process_node = params.get('init_process')
                            if init_process_node is None:
                                init_process_node = DAGNode()
                                init_process_node.name = 'init_process'
                                init_process_node.type = 'process'
                                init_process_node.parent = None
                                init_process_node.description = '初始化流程状态'
                                init_process_node.path = '/init_processes'
                                params.reset('init_process', init_process_node)
                            
                            init_process_node.children.append(self)
                            break
            else:
                print_and_log(
                    'cover_change',
                    "将建立流程:[{}]".format( self.name )
                )
                self.ds_modify_desc = 'create'
                params.next('changed_process_count')

                for child in self.children:
                    if child.type in ('self_dependent', 'dependent'):
                        # 增加新流程的初始化调用流程, 手动调用后手动删除
                        init_process_node = params.get('init_process')
                        if init_process_node is None:
                            init_process_node = DAGNode()
                            init_process_node.name = 'init_process'
                            init_process_node.type = 'process'
                            init_process_node.parent = None
                            init_process_node.description = '初始化流程状态'
                            init_process_node.path = '/init_processes'
                            params.reset('init_process', init_process_node)
                        
                        init_process_node.children.append(self)
                        break

    def measure_path(self):
        if self.path:
            return self.path
        
        if self.parent:
            self.path = self.parent.measure_path() + '/' + self.name
        else:
            self.path = '/' + self.name
        
        return self.path

    def merge_ds_node(self, params: AutoParams):
        if self.type == "process":
            for child_node in self.children:
                if child_node.type == "process":
                    child_node.merge_ds_node(params)
            
            if self.ds_modify_desc:
                result = ds_api.update_process_definition_by_code(
                    self.ds_project_code, self.ds_process_code, self.ds_process, True
                )
                print_and_log(
                    "merge",
                    "已写入流程:{}, 代码:{}, 含 {} 个节点, 建立空任务:{}".format(
                        self.name,
                        self.ds_process_code,
                        len(self.children),
                        params.get("is_generate_void"),
                    )
                )
        else:
            raise TypeError("must be process")

    def create_fake_instance(self, params: AutoParams):
        
        '''
        params:
            connection
            schedule_time
            start_time
            end_time
            host
            biz_date
            executor_id
            environment_code
        '''
        
        if self.type != "process":
            raise TypeError("must be process")
        
        connection = params.get('connection')
        
        if self.ds_modify_desc:
            
            has_self_dependent = False

            for child_node in self.children:
                if child_node.type == 'self_dependent':
                    has_self_dependent = True
                    break
            
            if has_self_dependent:
                # generate fake process instance
                process_instance_id = ds_db.gen_process_instance_id(connection)
                process_instance = {
                    "id": process_instance_id,
                    "name": "{}-fake-{}".format(self.name, params.get('schedule_time')),
                    "process_definition_code": self.ds_process_code,
                    "process_definition_version": 1, # un operatable
                    "state": 7,
                    "recovery": 0,
                    "start_time": params.get('start_time'),
                    "end_time": params.get('end_time'),
                    "run_times": 1,
                    "host": params.get('host'), #"172.2.1.22:5678",
                    "command_type": 6,
                    "command_param": None,
                    "task_depend_type": 2,
                    "max_try_times": 0,
                    "failure_strategy": 1,
                    "warning_type": 0,
                    "warning_group_id": 0,
                    "schedule_time": params.get('schedule_time'),
                    "command_start_time": params.get('start_time'),
                    "global_params": '[{{"prop":"global_bizdate","direct":"IN","type":"VARCHAR","value":"{biz_date}"}}]'.format(biz_date=params.get('biz_date')),
                    "process_instance_json": None,
                    "flag": 1,
                    "update_time": None,
                    "is_sub_process": 0,
                    "executor_id": params.get('executor_id'), #2,
                    "history_cmd": "SCHEDULER",
                    "dependence_schedule_times": None,
                    "process_instance_priority": 2,
                    "worker_group": "default",
                    "environment_code": "-1",
                    "timeout": 0,
                    "tenant_id": 1,
                    "var_pool": "[]",
                    "dry_run": 0,
                    "next_process_instance_id": 0,
                    "restart_time": params.get('start_time')
                }

                ds_db.insert_to_table(connection, 't_ds_process_instance', process_instance)

                print_and_log(
                    "instance",
                    "writed process_instance: id:{}, name:{}, code:{}".format(
                        process_instance_id,
                        process_instance['name'],
                        self.ds_process_code
                    )
                )
            
                for child_node in self.children:
                    task_instance_id = ds_db.gen_task_instance_id(connection)
                    task_instance = {
                        "id": task_instance_id,
                        "name": "{}-fake-{}".format(child_node.name, params.get('schedule_time')),
                        "task_type": child_node.ds_task['taskType'],
                        "task_code": child_node.ds_task_code,
                        "task_definition_version": 1,
                        "process_instance_id": process_instance_id,
                        "state": 7,
                        "submit_time": None,
                        "start_time": params.get('start_time'),
                        "end_time": params.get('end_time'),
                        "host": params.get('host'),
                        "execute_path": None,
                        "log_path": "",
                        "alert_flag": 0,
                        "retry_times": 0,
                        "pid": 0,
                        "app_link": None,
                        "task_params": "{}",
                        "flag": 1,
                        "retry_interval": 1,
                        "max_retry_times": 0,
                        "task_instance_priority": 2,
                        "worker_group": "default",
                        "environment_code": params.get('environment_code'),
                        "environment_config": "",
                        "executor_id": 2,
                        "first_submit_time": None,
                        "delay_time": 0,
                        "var_pool": None,
                        "dry_run": 0
                    }

                    ds_db.insert_to_table(connection, 't_ds_task_instance', task_instance)

                    print_and_log(
                        "instance",
                        "写入流程实例: id:{}, name:{}, code:{}".format(
                            task_instance_id,
                            task_instance['name'],
                            child_node.ds_task_code
                        )
                    )

        for child_node in self.children:
            if child_node.type == "process":
                child_node.create_fake_instance(params)
