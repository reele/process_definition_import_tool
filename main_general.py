#!/usr/bin/python3
# -*- coding: utf-8 -*-


import ds_generator
import general_scripts
from etl_node import EtlNode

SELF_DEPENDENT_CYCLE_MAPPINGS = {
    'D0': ['day', 'last1Days', 'D'],
    'M-1': ['month', 'lastMonthEnd', 'ME'],
    'M0': ['month', 'lastMonthBegin', 'MB'],
}


# 组映射关系
PROCESS_RELATIONS = {
    'CVT': ['PROCESS_CSO', 'CSO_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}贴源层作业流'],
    'ODB': ['PROCESS_CSO', 'CSO_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}贴源层作业流'],
    'SDB': ['PROCESS_CSO', 'CSO_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}贴源层作业流'],

    'PROCESS_CSO': ['PROCESS_FCSO', 'FCSO_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}贴源层作业流'],
    'FLG': ['PROCESS_FCSO', 'FCSO_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}贴源层作业流'],
    'PROCESS_FCSO': ['TRIGGER_FCSO', 'TRIGGER_{cycle_group}_DW_FCSO', '{cycle_group}贴源层触发做业组'],

    'PDB': ['PROCESS_PDB', 'PDB_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}整合层作业流'],
    'PROCESS_PDB': ['TRIGGER_PDB', 'TRIGGER_{cycle_group}_DW_PDB', '{cycle_group}_{theme_name}整合层触发做业组'],

    'CDB': ['PROCESS_CDB', 'CDB_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}汇总层作业流'],
    'PROCESS_CDB': ['TRIGGER_CDB', 'TRIGGER_{cycle_group}_DW_CDB', '{cycle_group}_{theme_name}汇总层触发做业组'],

    'DDB': ['PROCESS_DDB', 'DDB_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}数据下发作业流'],
    'PROCESS_DDB': ['TRIGGER_DDB', 'TRIGGER_{cycle_group}_DW_DDB', '{cycle_group}_{theme_name}数据下发触发做业组'],

    'TRIGGER_FCSO': ['TRIGGER', 'TRIGGER_{cycle_group}_DW', 'DW_{cycle_group}触发做业组'],
    'TRIGGER_PDB': ['TRIGGER', 'TRIGGER_{cycle_group}_DW', 'DW_{cycle_group}触发做业组'],
    'TRIGGER_CDB': ['TRIGGER', 'TRIGGER_{cycle_group}_DW', 'DW_{cycle_group}触发做业组'],
    'TRIGGER_DDB': ['TRIGGER', 'TRIGGER_{cycle_group}_DW', 'DW_{cycle_group}触发做业组'],
}

# 作业流数量过多时会导致张勇大量线程，弃用
# PROCESS_RELATIONS_ALL_SUB = {
#     'CVT': ['PROCESS_CSO', 'CSO_{job_name}', '贴源层作业流{job_name}'],
#     'ODB': ['PROCESS_CSO', 'CSO_{job_name}', '贴源层作业流{job_name}'],
#     'SDB': ['PROCESS_CSO', 'CSO_{job_name}', '贴源层作业流{job_name}'],
#     'PROCESS_CSO': ['PROCESS_GROUP_FCSO', 'TRIGGER_FCSO_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}贴源层触发做业组'],

#     'FLG': ['PROCESS_FLG', 'FLG_{job_name}', '贴源层作业流{job_name}'],
#     'PROCESS_FLG': ['PROCESS_GROUP_FCSO', 'TRIGGER_FCSO_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}贴源层触发做业组'],

#     'PROCESS_GROUP_FCSO': ['TRIGGER_GROUP_FCSO', 'TRIGGER_GROUP_{cycle_group}_FCSO', '{cycle_group}贴源层主题触发做业组'],

#     'PDB': ['PROCESS_PDB', 'PDB_{job_name}', '整合层作业流{job_name}'],
#     'PROCESS_PDB': ['PROCESS_GROUP_PDB', 'TRIGGER_PDB_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}整合层触发做业组'],
#     'PROCESS_GROUP_PDB': ['TRIGGER_GROUP_PDB', 'TRIGGER_GROUP_{cycle_group}_PDB', '{cycle_group}整合层主题触发做业组'],

#     'CDB': ['PROCESS_CDB', 'CDB_{job_name}', '汇总层作业流{job_name}'],
#     'PROCESS_CDB': ['PROCESS_GROUP_CDB', 'TRIGGER_CDB_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}汇总层触发做业组'],
#     'PROCESS_GROUP_CDB': ['TRIGGER_GROUP_CDB', 'TRIGGER_GROUP_{cycle_group}_CDB', '{cycle_group}汇总层主题触发做业组'],

#     'DDB': ['PROCESS_DDB', 'DDB_{job_name}', '数据下发作业流{job_name}'],
#     'PROCESS_DDB': ['PROCESS_GROUP_DDB', 'TRIGGER_DDB_{cycle_group}_{theme_name}', '{cycle_group}_{theme_name}汇总层触发做业组'],
#     'PROCESS_GROUP_DDB': ['TRIGGER_GROUP_DDB', 'TRIGGER_GROUP_{cycle_group}_DDB', '{cycle_group}数据下发触发做业组'],
# }


def get_process_relation(group, job_name, theme_name, cycle_group):
    process_relation = PROCESS_RELATIONS.get(group)
    if process_relation:
        process_relation = process_relation.copy()
        process_relation[1] = process_relation[1].format(
            job_name=job_name, theme_name=theme_name, cycle_group=cycle_group)
        process_relation[2] = process_relation[2].format(
            job_name=job_name, theme_name=theme_name, cycle_group=cycle_group)
    return process_relation


def get_node_path(group, job_name, theme_name, cycle_group):
    process_relation = get_process_relation(
        group, job_name, theme_name, cycle_group)
    if process_relation:
        return get_node_path(process_relation[0], job_name, theme_name, cycle_group) + '/' + process_relation[1]
    return ''


if __name__ == "__main__":

    ds_generator.get_db_info()

    root_node = EtlNode('root', 'root')
    nodes = {'root': root_node}
    task_cycle_mappings = {}
    task_names = set()

    for s_row in general_scripts.etl_jobs:
        group_name = s_row[0]
        theme_name = s_row[1]
        job_name = s_row[2]
        description = s_row[3]
        task_name = '{}_{}_{}'.format(group_name, theme_name, job_name)

        shell_command = '{app_name} {script_path}{separator}{script_name} {bizdate8}'.format(
            bizdate8='${bizdate}',
            app_name=s_row[6],  # 脚本应用名称
            script_path=s_row[9],  # 脚本路径
            separator='/' if s_row[9][-1] != '/' else '',  # 路径分隔符
            script_name=s_row[8],  # 脚本名称
        )

        self_dependent_cycle_mapping = SELF_DEPENDENT_CYCLE_MAPPINGS[s_row[4] + s_row[5]]
        cycle_group = self_dependent_cycle_mapping[2]
        task_cycle_mappings[task_name] = cycle_group

        path = get_node_path(group_name, job_name, theme_name,
                             cycle_group) + '/' + task_name
        task_node = nodes.get(path)
        if task_node:
            raise ValueError('path:' + path)
        task_node = EtlNode()
        nodes[path] = task_node

        task_node.name = task_name
        task_node.type = 'shell'
        task_node.command = shell_command
        task_node.description = description
        task_node.path = path
        task_node.cycle = self_dependent_cycle_mapping[0]
        task_node.continuous_check_date = self_dependent_cycle_mapping[1]
        task_node.cycle_group = cycle_group
        task_node.group_name = group_name

        # day : today | last1Days
        # month : lastMonthBegin | lastMonthEnd
        # week : lastMonday

        nodes[task_node.path] = task_node

        schedule_group = group_name
        last_node = task_node
        last_node_is_new = True

        while True:
            process_relation = get_process_relation(
                schedule_group, job_name, theme_name, cycle_group)
            if not process_relation:
                if last_node_is_new:
                    root_node.children.append(last_node)
                break

            schedule_group = process_relation[0]
            process_name = process_relation[1]
            process_description = process_relation[2]

            path = get_node_path(schedule_group, job_name,
                                 theme_name, cycle_group) + '/' + process_name
            process_node = nodes.get(path)
            if not process_node:
                process_node = EtlNode()
                process_node.name = process_name
                process_node.type = 'process'
                process_node.parent = root_node
                process_node.description = process_description
                process_node.path = path
                nodes[process_node.path] = process_node
                if last_node_is_new:
                    process_node.children.append(last_node)
                    last_node.parent = process_node
                last_node_is_new = True
            else:
                if last_node_is_new:
                    process_node.children.append(last_node)
                    last_node.parent = process_node
                last_node_is_new = False
            last_node = process_node

    # 节点依赖
    for d_row in general_scripts.etl_dependencies:

        group_name, theme_name, job_name = d_row[0], d_row[1], d_row[2]
        dependency_group_name, dependency_theme_name, dependency_job_name = d_row[
            3], d_row[4], d_row[5]

        task_name = '{}_{}_{}'.format(group_name, theme_name, job_name)
        dependency_task_name = '{}_{}_{}'.format(
            dependency_group_name, dependency_theme_name, dependency_job_name)

        task_cycle_group = task_cycle_mappings.get(task_name)
        if not task_cycle_group:
            print(
                'dependent [* {}]->[{}] not exists'.format(task_name, dependency_task_name))
            continue
        dependency_task_cycle_group = task_cycle_mappings.get(
            dependency_task_name)
        if not dependency_task_cycle_group:
            print(
                'dependency [{}]->[* {}] not exists'.format(task_name, dependency_task_name))
            continue

        task_path = get_node_path(
            group_name, job_name, theme_name, task_cycle_group) + '/' + task_name

        dependency_task_path = get_node_path(
            dependency_group_name, dependency_job_name, dependency_theme_name, dependency_task_cycle_group) + '/' + dependency_task_name

        orig_task_node = nodes.get(task_path)
        if not orig_task_node:
            print('dependent [{}] not exist'.format(task_path))
            continue

        orig_dependency_task_node = nodes.get(dependency_task_path)
        if not orig_dependency_task_node:
            print('dependency [{}] not exist'.format(dependency_task_path))
            continue

        t_level = task_path.count('/')
        d_level = dependency_task_path.count('/')
        if t_level > d_level:
            while t_level > d_level:
                task_path = task_path[:task_path.rindex('/')]
                t_level -= 1

        task_node = nodes.get(task_path)
        if not task_node:
            print('dependent [{}] not exist'.format(task_path))
            continue

        dependency_task_node = nodes.get(dependency_task_path)
        if not dependency_task_node:
            print('dependency [{}] not exist'.format(dependency_task_path))
            continue

        # if task_node.name.startswith('PDB'):
        #     stop=True
        # ...<作业主题<作业层<周期组
        # ∴作业主题内可通过DAG依赖: level >= 3
        while t_level >= 3 and t_level >= d_level:
            if task_node.parent == dependency_task_node.parent:
                # 同一组下使用DAG
                if dependency_task_node not in task_node.pre_nodes:
                    task_node.pre_nodes.append(dependency_task_node)
                    dependency_task_node.dependents.add(task_node)
                break
            task_node = task_node.parent
            dependency_task_node = dependency_task_node.parent
            t_level -= 1
            d_level -= 1
        else:
            # 建立依赖节点
            path = orig_task_node.parent.path + '/' + dependency_task_name
            dep_node = nodes.get(path)
            if not dep_node:
                dep_node = EtlNode()
                nodes[path] = dep_node
                dep_node.name = orig_dependency_task_node.name
                dep_node.type = 'dependent'
                dep_node.dependency_node = orig_dependency_task_node
                dep_node.path = path
                dep_node.cycle = 'day'
                dep_node.continuous_check_date = 'today'
                dep_node.parent = orig_task_node.parent
                dep_node.parent.children.append(dep_node)
                dep_node.dependents.add(orig_task_node)
                dep_node.description = orig_dependency_task_node.description
                orig_dependency_task_node.dependents.add(dep_node)

            orig_task_node.pre_nodes.append(dep_node)
            orig_task_node.parent.children.append(dep_node)

    # 自依赖,加到'ODB','SDB','PDB'每个作业流的头部任务之前
    for task_node in [node for node in nodes.values()]:
        if task_node.type in ('process', 'self_dependent', 'dependent'):
            continue
        if task_node.name == 'root':
            continue
        if task_node.group_name not in ('ODB', 'SDB', 'PDB', 'CDB', 'STS'):
            continue

        path = task_node.parent.path + '/' + task_node.name + \
            '_' + task_node.continuous_check_date
        cycle_dep_node = nodes.get(path)
        if cycle_dep_node:
            raise ValueError('path :'+path)
        cycle_dep_node = EtlNode()
        nodes[path] = cycle_dep_node

        cycle_dep_node.type = 'self_dependent'
        cycle_dep_node.dependency_node = task_node
        cycle_dep_node.path = path
        cycle_dep_node.name = task_node.name + '_' + task_node.continuous_check_date
        cycle_dep_node.cycle = task_node.cycle
        cycle_dep_node.continuous_check_date = task_node.continuous_check_date
        cycle_dep_node.parent = task_node.parent
        cycle_dep_node.description = task_node.description

        cycle_dep_node.pre_nodes = task_node.pre_nodes

        task_node.pre_nodes = [cycle_dep_node]
        task_node.parent.children.append(cycle_dep_node)

    process_definitions = {}

    root_node.gen_ds_node(process_definitions)

    ds_generator.refresh_process_definition_reference(process_definitions)

    ds_generator.generate_process_definition_json(process_definitions)

    ds_generator.merge_process_definitions_to_db(process_definitions)
