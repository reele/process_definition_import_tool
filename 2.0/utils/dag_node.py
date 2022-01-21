#!/usr/bin/python3

from datetime import datetime
import json
from typing import Any
from . import ds_api


class AutoParams():
    def __init__(self, seqs: dict[str, Any]):
        self.params = seqs

    def next(self, key):
        value = self.params[key]
        if isinstance(value, int):
            self.params[key] = value + 1
            return value
        elif isinstance(value, list):
            return value.pop()
        else:
            raise TypeError()

    def get(self, key):
        value = self.params[key]
        if isinstance(value, int):
            return value
        elif isinstance(value, str):
            return value
        elif isinstance(value, list):
            return value[-1]
        else:
            raise TypeError()

    def pick(self, params_key, key):
        value = self.params[params_key]
        if isinstance(value, dict):
            return value.pop(key)
        else:
            raise TypeError()


class DAGNode(object):
    class DAGJsonEncoder(json.JSONEncoder):

        def default_children(self, o):
            if isinstance(o, DAGNode):
                node_object = {
                    'name': o.name,
                    'type': o.type,
                    'cycle': o.cycle,
                    'path': o.path,
                    'prev_nodes': [node.name for node in o.prev_nodes],
                    'next_nodes': [node.name for node in o.next_nodes],
                }

                return node_object

        def default(self, o):
            try:
                if isinstance(o, DAGNode):
                    node_object = {
                        'name': o.name,
                        'type': o.type,
                        'cycle': o.cycle,
                        'path': o.path,
                        'prev_nodes': [node.name for node in o.prev_nodes],
                        'next_nodes': [node.name for node in o.next_nodes],
                        'children': [self.default_children(child) for child in o.children],
                    }
            except TypeError:
                pass
            else:
                return node_object
            # Let the base class default method raise the TypeError
            return json.JSONEncoder.default(self, o)

    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    TASK_DEF_TYPE_MAPPING = {
        'process': 'SUB_PROCESS',
        'shell': 'SHELL',
        'dependent': 'DEPENDENT',
        'self_dependent': 'DEPENDENT',
    }

    def __init__(self, type: str = None, name: str = None):
        self.group_name: str = None
        self.type: str = type
        self.name: str = name
        self.children: list[DAGNode] = []
        self.parent: DAGNode = None
        self.dependents: set[DAGNode] = set()
        self.dependency_node: DAGNode = None
        self.command: str = None
        self.prev_nodes: list[DAGNode] = []
        self.next_nodes: list[DAGNode] = []
        self.description: str = None
        self.path: str = None
        self.cycle: str = None
        self.continuous_check_date: str = None
        self.cycle_group: str = None
        self.task_code: int = 0
        self.process_code: int = 0
        self.task_id: int = 0
        self.process_id: int = 0
        self.project_code: int = 0
        self.ds_node: dict = None
        self.ds_relations: list[dict] = None
        self.ds_locations: list[dict] = None
        self.ds_task: dict = None
        self.ds_process: dict = None

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

    def gen_processes_and_codes(self, params: AutoParams):
        self.project_code = params.get('project_code')

        if self.parent.type == 'process':
            # 是流程下的子任务, 生成任务编号
            task_idendifier = '{process_name}:{task_name}:{task_type}'.format(
                process_name=self.parent.name,
                task_name=self.name,
                task_type=DAGNode.TASK_DEF_TYPE_MAPPING[self.type],
            )
            try:
                data = params.pick('old_tasks', task_idendifier)
            except KeyError:
                code = ds_api.generate_task_codes(1)[0]
                data = {'code': code}
                print('created task code {}::{} .'.format(
                    task_idendifier, code))

            self.task_code = data['code']
            self.task_id = params.next('id')

        if self.type == 'process':
            # 按名字取出已有的process, 不存在时新建
            try:
                data = params.pick('old_process', self.name)
            except KeyError:
                data = ds_api.create_empty_process_definition(
                    self.project_code, self.name, params.get('tenant_name'))
                print('created empty process {} code:{} id:{}.'.format(
                    self.name, self.process_code, self.process_id))

            self.process_code = data['code']
            self.process_id = data['id']

            for child_node in self.children:
                child_node.gen_processes_and_codes(params)

    def gen_ds_task_node(self, params: AutoParams):

        if self.type == 'process':
            task_type = 'SUB_PROCESS'
            task_description = self.description
            task_params = {
                "processDefinitionCode": self.process_code,
                "dependence": {},
                "conditionResult": {
                    "successNode": [],
                    "failedNode": []
                },
                "waitStartTimeout": {},
                "switchResult": {}
            }
        elif self.type == 'shell':
            task_type = 'SHELL'
            task_description = self.description
            task_params = {
                "localParams": [
                    {
                        "prop": "bizdate",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "${global_bizdate}"
                    }
                ],
                "resourceList": [],
                "rawScript": 'echo \'{}\''.format(self.command.replace('\'','\\\'')) if params.get('is_first_generation') else self.command,
                "dependence": {},
                "conditionResult": {
                    "successNode": [],
                    "failedNode": []
                },
                "waitStartTimeout": {},
                "switchResult": {}
            }
        elif self.type == 'dependent' or self.type == 'self_dependent':
            task_type = 'DEPENDENT'
            task_description = self.description + '_' + self.continuous_check_date
            task_params = {
                "localParams": [
                    {
                        "prop": "bizdate",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "${global_bizdate}"
                    }
                ],
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "projectCode": self.project_code,
                                    "definitionCode": self.dependency_node.parent.process_code,
                                    "depTaskCode": self.dependency_node.task_code,
                                    "cycle": self.cycle,
                                    "dateValue": self.continuous_check_date
                                }
                            ]
                        }
                    ]
                },
                "conditionResult": {
                    "successNode": [],
                    "failedNode": []
                },
                "waitStartTimeout": {
                    "strategy": "FAILED",
                    "interval": None,
                    "checkInterval": None,
                    "enable": False
                },
                "switchResult": {}
            }
        else:
            raise TypeError('Unknown type')

        task_priority = 'HIGH' if self.dependents else 'MEDIUM'

        self.ds_task = {
            "id": self.task_id,
            "code": self.task_code,
            "name": self.name,
            "version": 1,
            "description": task_description,
            "projectCode": self.project_code,
            "userId": 1,
            "taskType": task_type,
            "taskParams": task_params,
            "taskParamList": [],
            "taskParamMap": {},
            "flag": 'NO' if task_type == 'DEPENDENT' and params.get('is_first_generation') else 'YES',
            "taskPriority": task_priority,
            "userName": None,
            "projectName": None,
            "workerGroup": 'default',
            "environmentCode": -1,
            "failRetryTimes": 0,
            "failRetryInterval": 1,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": None,
            "timeout": 0,
            "delayTime": 0,
            "resourceIds": "",
            "createTime": datetime.now().strftime(DAGNode.TIME_FORMAT),
            "updateTime": datetime.now().strftime(DAGNode.TIME_FORMAT),
            "modifyBy": None
        }

        return self.ds_task

    def gen_task_locations(self) -> dict:

        locations = []

        for child in self.children:
            child.reset_optimize_data()

        # 节点分组
        def gen_task_locations_clear_connected_nodes(node: DAGNode) -> dict[str, DAGNode]:
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
        def group_sort_key(nodes): return len(nodes)
        groups.sort(key=group_sort_key, reverse=True)

        begin_yoff = 0

        # 优化跨层级节点
        def find_redundant_chain(base_node: DAGNode, check_node: DAGNode) -> list[DAGNode]:

            for next_node in base_node.next_nodes:
                if next_node.dag_level + 1 == check_node.dag_level and check_node in next_node.next_nodes:
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
                        print(
                            'removed redundant chain [{}]->[{}] by:'.format(base_node.name, next_node.name))
                        print('    {}'.format(
                            '->'.join(['[{}]'.format(n.name) for n in result])
                        ))

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
                no_prev_list = [e for e in group_level_list.values()
                                if not e.optimize_prev]
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

            def node_sort_key(node): return (
                node.count_prev) + (node.count_next)

            node_offset = 0

            for node_in_level in node_in_levels:
                node_in_level.sort(key=node_sort_key, reverse=True)

                for dag_node in node_in_level:

                    # size = 220 * 50
                    location = {
                        "taskCode": dag_node.task_code,
                        "x": 300 * xoff + 100 + node_offset * 10,
                        "y": 100 * yoff + 100
                    }

                    locations.append(location)

                    yoff += 1
                    node_offset += 1

                if max_hight < yoff:
                    max_hight = yoff

                xoff += 1
                yoff = begin_yoff

            begin_yoff = max_hight

        return locations

    def gen_task_relations(self, seqs: AutoParams):
        relations = []
        for child in self.children:
            for prev_node in child.prev_nodes:
                relations.append(
                    {
                        "id": seqs.next('id'),
                        "name": "",
                        "processDefinitionVersion": 1,
                        "projectCode": self.project_code,
                        "processDefinitionCode": self.process_code,
                        "preTaskCode": prev_node.task_code,
                        "preTaskVersion": 1,
                        "postTaskCode": child.task_code,
                        "postTaskVersion": 1,
                        "conditionType": "NONE",
                        "conditionParams": {},
                        "createTime": datetime.now().strftime(DAGNode.TIME_FORMAT),
                        "updateTime": datetime.now().strftime(DAGNode.TIME_FORMAT)
                    }
                )
            if not child.prev_nodes:
                relations.append(
                    {
                        "id": seqs.next('id'),
                        "name": "",
                        "processDefinitionVersion": 1,
                        "projectCode": self.project_code,
                        "processDefinitionCode": self.process_code,
                        "preTaskCode": 0,
                        "preTaskVersion": 1,
                        "postTaskCode": child.task_code,
                        "postTaskVersion": 1,
                        "conditionType": "NONE",
                        "conditionParams": {},
                        "createTime": datetime.now().strftime(DAGNode.TIME_FORMAT),
                        "updateTime": datetime.now().strftime(DAGNode.TIME_FORMAT)
                    }
                )

        return relations

    def gen_ds_node(self, params: AutoParams):
        if self.type == 'process':
            ds_task_nodes = []
            for child_node in self.children:
                if child_node.type == 'process':
                    child_node.gen_ds_node(params)
                    ds_task_nodes.append(child_node.gen_ds_task_node(params))
                else:
                    ds_task_nodes.append(child_node.gen_ds_node(params))

            self.ds_locations = self.gen_task_locations()

            self.ds_relations = self.gen_task_relations(params)

            self.ds_process = {
                'code': self.process_code,
                'locations': json.dumps(self.ds_locations),
                'name': self.name,
                'projectCode': self.project_code,
                'taskDefinitionJson': json.dumps(ds_task_nodes),
                'taskRelationJson': json.dumps(self.ds_relations),
                'tenantCode': 'etl',
                'description': self.description,
                'globalParams': json.dumps([{'prop': 'global_bizdate', 'direct': 'IN', 'type': 'VARCHAR', 'value': '${system.biz.date}'}]),
                'releaseState': 'ONLINE',
                'timeout': 0,
            }

            result = ds_api.update_process_definition_by_code(
                self.project_code, self.process_code, self.ds_process, True)
            print('writed process:{}, code:{} with {} tasks, is_first_generation:{}'.format(
                self.name, self.process_code, len(self.children),
                params.get('is_first_generation')
                ))

        else:
            self.ds_node = self.gen_ds_task_node(params)
            return self.ds_node

    def import_to_ds_server(self, project_name: str, tenant_name: str, is_first_generation: bool = False):
        '''
            * 因为数仓任务涉及业务连续性问题, 不允许跨日执行, 大部分作业节点必须有前一天的任务自依赖,
            * 但迁移后首次执行时不能有依赖节点, 否则会等待不存在的任务实例(空跑仍然会等待依赖节点)
            * 以上问题有三种解决办法:
            1. 生成两次任务, 第一次生成时不生成依赖节点, 全部空跑一次, 再在保留原任务code的基础上全部更新一次作业定义
            2. 作业一次性生成, 通过api禁用所有依赖节点, 全部空跑一次后再启用依赖节点(但是目前 'api:releaseTaskDefinition' 似乎没有对流程中的任务定义生效 2.0.2-r)
            3. 第一次生成时依赖节点状态为失效, shell命令替换为echo, 全部跑一次, 再将流程定义全部更新一次(使用此方式)

            * 由于dependent任务和sub_process任务的参数中,指向其他作业的code在导入时无法自动映射,
            * 故必须使用api获取已存在的流程或提前生成有效的空流程, 并直接将实际存在的流程code引用到参数中.
            * 作业生成流程:
            1. 用名称匹配项目下已有的流程code, 不存在时建立空流程; 删除未匹配到的旧流程.
            2. 用名称匹配项目下已有的任务code, 不存在时生成新任务code, 必须保证 '流程名称,任务名称,任务类型' 唯一
            3. 遍历dag生成流程, 将流程定义更新到服务中
        '''
        if self.type != 'root':
            raise ValueError('must be root node to call this method.')

        project_code = ds_api.get_project_code_by_name(project_name)

        if project_code == 0:
            result = ds_api.create_project(project_name, '')
            project_code = result['code']

        # 导出项目下的所有流程
        process_def_list = ds_api.get_process_definitions_by_project_code(
            project_code)

        processes: dict[str, int] = {}
        tasks: dict[str, int] = {}

        for process_def in process_def_list:
            process_def_main = process_def['processDefinition']
            task_def_list = process_def['taskDefinitionList']
            process_name = process_def_main['name']
            processes[process_name] = process_def_main
            for task_def in task_def_list:
                task_idendifier = '{process_name}:{task_name}:{task_type}'.format(
                    process_name=process_name,
                    task_name=task_def['name'],
                    task_type=task_def['taskType'],
                )
                tasks[task_idendifier] = task_def

        params = AutoParams(
            {
                # 'task_code': task_codes,
                'project_code': project_code,
                'id': 1,
                'tenant_name': tenant_name,
                'old_process': processes,
                'old_tasks': tasks,
                'is_first_generation': is_first_generation
            }
        )

        # 用流程名称匹配项目下已有的流程code, 存在时从old_process列表移出, 不存在时建立process.
        # 遍历每个任务节点领取通过api提前生成的任务code
        for child_node in self.children:
            child_node.gen_processes_and_codes(params)

        for child_node in self.children:
            child_node.gen_ds_node(params)

        # 清理无效的流程
        if processes:
            for process in processes.values():
                ds_api.delete_process_by_code(
                    project_code, process['code'], True, process['name'])
                print('deleted process:{}, code:{}.'.format(
                    process['name'], process['code']))
