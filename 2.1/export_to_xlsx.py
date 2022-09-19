#!/usr/bin/python3

import sys
from typing import Any
from datetime import datetime, timedelta
from dag_node import DAGNode
from simple_log import print_and_log, LOG_TIME_STR
import ds_api
from schedule_config import ScheduleConfig
from auto_params import AutoParams
import ds_db

from openpyxl.worksheet.worksheet import Worksheet
from openpyxl import Workbook, load_workbook

from main_general import load_server_definitions_to_dag, load_remote


CYCLE_MAPPING = {
    'day': 'D',
    'month': 'M',
    'week': 'W',
    'quart': 'Q',
}


def get_all_endpoint_dependency_nodes(node: DAGNode, dependency_nodes: set[DAGNode]=None):
    """遍历所有被依赖节点的末端节点"""

    if node.type == 'shell':
        dependency_nodes.add(node)
    elif node.type == 'dependent':
        get_all_endpoint_dependency_nodes(node.dependency_node, dependency_nodes)
    elif node.type == 'process':
        for child in node.children:
            if len(child.next_nodes) == 0:
                get_all_endpoint_dependency_nodes(child, dependency_nodes)
    elif node.type == 'self_dependent':
        pass
    else:
        assert False

def is_dag_head_node(node: DAGNode):
    if len(node.prev_nodes) > 0:
        for prev_node in node.prev_nodes:
            if prev_node.type not in ('dependent', 'self_dependent'):
                return False
            
            prev_dependent_is_dag_head_node = is_dag_head_node(prev_node)
            if not prev_dependent_is_dag_head_node:
                return False

    return True

def expand_all_dependences(node: DAGNode, dependent_rows: list, dependency_nodes: list[DAGNode]=None):
    """子工作流有依赖时, 将依赖传递到所有下级shell节点"""
    
    if node.type == 'shell':
        for dependency_node in dependency_nodes:
            dependent_rows.append([
                node.group_code,
                node.name,
                dependency_node.group_code,
                dependency_node.name,
            ])
    elif node.type == 'process':
        for child in node.children:
            if child.type in ('shell', 'process'):
                # 仅关联DAG头部作业
                if is_dag_head_node(child):
                    expand_all_dependences(child, dependent_rows, dependency_nodes)
            else:
                pass


def export_to_xlsx(project_name, xlsx_path: str):

    config = ScheduleConfig.from_xlsx('schedule_config_template.xlsx')

    root_node = DAGNode("root", "root")
    tree_nodes: dict[str, DAGNode] = {"root": root_node}

    project_code, origin_processes = load_remote(project_name)
    assert True if project_code else False

    print_and_log(None, "从服务器加载项目定义:[{}] ...".format(project_name))
    
    load_server_definitions_to_dag(tree_nodes, origin_processes, config)
    print_and_log(
        None, "导入共计 [{}] 个节点.".format(len(tree_nodes))
    )

    task_rows = []
    dependent_rows = []


    for path, node in tree_nodes.items():
        
        if node.max_dag_dep_level == 0 or node.max_dag_dep_level is None:
            node.max_dag_dep_level = 3
        if node.group_code == 'FLG':
            node.max_dag_dep_level = 2
        
        if node.type == 'shell':
            # 生成shell作业配置
            task_rows.append([
                node.group_code,                            # 分组编码
                node.name,                                  # 名称
                node.description,                           # 描述
                CYCLE_MAPPING[node.cycle],                  # 执行周期(D/MB/ME)
                'Y' if node.continuous_check_date else 'N', # 是否自依赖上一周期(Y/N)
                node.command,                               # 脚本内容 (支持多行)
                node.fail_retry_times,                      # 失败重试次数
                node.fail_retry_interval,                   # 失败重试间隔(分钟)
                node.max_dag_dep_level,                     # 最低依赖级别(DAG图的组依赖级别)
                '',                                         # 操作类型(ON/OFF/DELETE)
            ])

        if node.type in ('shell', 'process'):

            dependency_nodes = set()
            # 生成shell节点依赖
            for prev_node in node.prev_nodes:
                
                # 忽略自依赖节点
                if prev_node.type == 'self_dependent':
                    continue
                
                # 如果是子工作流节点, 遍历出所有末端节点
                get_all_endpoint_dependency_nodes(prev_node, dependency_nodes)

            # 关联所有下级节点的依赖
            expand_all_dependences(node, dependent_rows, dependency_nodes)


    book = load_workbook('调度配置模板.xlsx')

    task_sheet: Worksheet = book['作业列表']
    dependent_sheet: Worksheet = book['依赖列表']

    for row in task_rows:
        task_sheet.append(row)

    for row in dependent_rows:
        dependent_sheet.append(row)

    book.save(xlsx_path)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        config_xlsx_path = input('\n输入导出的调度配置路径:')
        export_to_xlsx(
            "[dw_main][1.1]", config_xlsx_path
        )
    else:
        export_to_xlsx(
            "[dw_main][1.1]", sys.argv[1]
        )
