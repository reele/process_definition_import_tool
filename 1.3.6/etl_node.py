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
                id,  # id - ??????
                self.name,  # name - ??????????????????
                1,  # version - ??????????????????
                1,  # release_state - ??????????????????????????????0 ????????? , 1?????????
                ds_generator.DB_INFO['project_id'],  # project_id - ??????id
                ds_generator.DB_INFO['user_id'],  # user_id - ????????????????????????id
                process_node,  # process_node - ????????????JSON
                self.description,  # description - ??????????????????
                ds_generator.GLOBAL_PARAMS,  # global_params - ????????????
                1,  # flag - ?????????????????????0 ????????????1 ??????
                locations,  # locations - ??????????????????
                connections,  # connects - ??????????????????
                '',  # receivers - ?????????
                '',  # receivers_cc - ?????????
                datetime.now(),  # create_time - ????????????
                0,  # timeout - ????????????
                ds_generator.DB_INFO['tenant_id'],  # tenant_id - ??????id
                datetime.now(),  # update_time - ????????????
                'ds_gen',  # modify_by - ????????????
                ''  # resource_ids - ??????ids
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
