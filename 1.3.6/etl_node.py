#!/usr/bin/python3
# -*- coding: utf-8 -*-

import ds_generator
from datetime import datetime


class EtlNode(object):
    """
    EtlNode
    """

    def __init__(self, type: str = None, name: str = None):
        self.group_name: str = None
        self.type: str = type
        self.name: str = name
        self.children: list[EtlNode] = []
        self.parent: EtlNode = None
        self.dependents: set[EtlNode] = set()
        self.dependency_node: EtlNode = None
        self.command: str = None
        self.pre_nodes: list[EtlNode] = []
        self.description: str = None
        self.path: str = None
        self.cycle: str = None
        self.continuous_check_date: str = None
        self.cycle_group: str = None

    def gen_ds_node(self, process_definitions: dict[str:dict]):

        # if self.name == 'ODB_S01_GLGLGGLG':
        #     stop=True

        if self.type == 'root':
            for child_node in self.children:
                child_node.gen_ds_node(process_definitions)
            return

        if self.type == 'process':
            task_dict = {}
            for child_node in self.children:
                if child_node.type == 'process':
                    child_node.gen_ds_node(process_definitions)
                    # if child_node.name == 'ODB_S01_GLGLGGLG':
                    #     stop=True
                    task_node = ds_generator.gen_sub_process_task_node(
                        child_node.name,
                        [EtlNode.name for EtlNode in child_node.pre_nodes],
                        child_node.description,
                        'HIGH' if child_node.dependents else 'MEDIUM'
                    )
                else:
                    task_node = child_node.gen_ds_node(None)
                ds_generator.add_node_to_task_dict(task_dict, task_node)

            locations = ds_generator.gen_locations(task_dict)

            connections = ds_generator.gen_connections(task_dict)

            process_node = ds_generator.gen_process_node(task_dict)

            id = ds_generator.get_exist_process_id(self.name)
            if id == -1:
                id = ds_generator.gen_process_definition_id()

            process_definition = ds_generator.gen_process_definition(
                id,  # id - 主键
                self.name,  # name - 流程定义名称
                1,  # version - 流程定义版本
                1,  # release_state - 流程定义的发布状态：0 未上线 , 1已上线
                ds_generator.DB_INFO['project_id'],  # project_id - 项目id
                ds_generator.DB_INFO['user_id'],  # user_id - 流程定义所属用户id
                process_node,  # process_node - 流程定义JSON
                self.description,  # description - 流程定义描述
                ds_generator.GLOBAL_PARAMS,  # global_params - 全局参数
                1,  # flag - 流程是否可用：0 不可用，1 可用
                locations,  # locations - 节点坐标信息
                connections,  # connects - 节点连线信息
                '',  # receivers - 收件人
                '',  # receivers_cc - 抄送人
                datetime.now(),  # create_time - 创建时间
                0,  # timeout - 超时时间
                ds_generator.DB_INFO['tenant_id'],  # tenant_id - 租户id
                datetime.now(),  # update_time - 更新时间
                'ds_gen',  # modify_by - 修改用户
                ''  # resource_ids - 资源ids
            )

            if process_definitions.get(self.name):
                raise ValueError('process name [{}] exists!'.format(self.name))
            process_definitions[self.name] = process_definition

        else:
            if self.type == 'shell':
                return ds_generator.gen_shell_task_node(
                    self.name,
                    self.command,
                    [EtlNode.name for EtlNode in self.pre_nodes],
                    self.description,
                    'HIGH' if self.dependents else 'MEDIUM'
                )
            elif self.type == 'dependent' or self.type == 'self_dependent':
                return ds_generator.gen_denpendent_task_node(
                    self.name,
                    [EtlNode.name for EtlNode in self.pre_nodes],
                    self.dependency_node.parent.name,
                    self.dependency_node.name,
                    self.cycle,
                    self.continuous_check_date,
                    self.description + '_' + self.continuous_check_date,
                    'MEDIUM'  # 'HIGH' if self.dependents else 'MEDIUM'
                )
