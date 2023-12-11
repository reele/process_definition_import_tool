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

PROJECT_NAMES = set()
ENVIRONMENTS = {}
ENVIRONMENT_NAMES = {}


def confirm(message, msg_yes, msg_no):
    while True:
        prompt = """=================================================
{message}
Input [yes] and Enter to continue.
Or input [no] and Enter to cancel.
=================================================
[yes/no]: """.format(
            message=message
        )
        value = input(prompt)
        value = value.lower()
        if value == "yes":
            if msg_yes:
                print(msg_yes)
            return True
        if value == "no":
            if msg_no:
                print(msg_no)
            return False


def load_config_to_dag(
    tree_nodes: dict[str, DAGNode],
    config: ScheduleConfig,
    origin_projects: dict[int, dict]
):
    # root_node = tree_nodes["root"]

    task_cycle_mappings = {}

    # 生成任务节点, 建立按分组关系建立分组节点
    for job_full_name, job_definition in config.jobs.items():
        project_node = tree_nodes[f'/{job_definition.project_name}']

        if job_definition.is_full_path():
            task_name = job_definition.name
        else:
            task_name = job_definition.full_name

        shell_command = job_definition.raw_script

        cycle = job_definition.cycle
        task_cycle_mappings[task_name] = cycle
        path = config.measure_job_path(job_definition) + "/" + task_name

        task_node = tree_nodes.get(path)
        if task_node is not None:
            if task_node.type != "shell" and job_definition.raw_script:
                print_and_log("配置错误", "作业 [{}] 已配置执行脚本, 但原作业类型是 [分组]".format(job_definition.full_name))
                print_and_log("配置错误", "冲突的路径: [{}]".format(path))
                raise ValueError("作业名称与组名称冲突")
            task_node.ds_modify_desc = "update"
            last_node_is_new = False
        else:
            task_node = DAGNode('shell')
            task_node.ds_modify_desc = "create"
            tree_nodes[path] = task_node
            last_node_is_new = True
            task_node.ds_project_code = project_node.ds_project_code
            task_node.project_name = project_node.name

        if job_definition.enabled == 'NO' and task_node.ds_modify_desc == 'create':
            print_and_log("配置错误", "需要下线的作业 [{}] 未定义".format(job_definition.full_name))
            raise ValueError("配置错误, 已中止.")
        
        task_node.flag = job_definition.enabled

        if job_definition.enabled == 'YES':
            task_node.project_name = job_definition.project_name
            task_node.name = task_name
            # task_node.type = "shell"
            task_node.command = shell_command
            task_node.description = job_definition.desc
            task_node.path = path
            task_node.cycle = job_definition.cycle.cycle_name
            task_node.ds_task_name = None  # 若节点已存在, 覆盖名称
            task_node.fail_retry_times = job_definition.fail_retry_times
            task_node.fail_retry_interval = job_definition.fail_retry_interval

            if not job_definition.self_dependent and task_node.continuous_check_date:

                if task_node.prev_nodes:
                    self_dependent_node = None
                    for node in task_node.prev_nodes:
                        if node.type == 'self_dependent':
                            self_dependent_node = node
                            break
                    if self_dependent_node is not None:
                        self_dependent_node.parent.children.remove(self_dependent_node)
                        task_node.prev_nodes.remove(self_dependent_node)
                        for prev_node in self_dependent_node.prev_nodes:
                            prev_node.next_nodes.remove(self_dependent_node)
                            prev_node.next_nodes.append(task_node)
                            task_node.prev_nodes.append(prev_node)
                        task_node.continuous_check_date = None
                        tree_nodes.pop(self_dependent_node.path)

            elif job_definition.self_dependent and not task_node.continuous_check_date:
                task_node.continuous_check_date = (
                    job_definition.cycle.self_dependent_type
                    if job_definition.self_dependent
                    else None
                )

            task_node.group_code = job_definition.group
            task_node.highest_dependent_level = job_definition.max_dependent_level

            
            # t_ds_task_definition.workerGroup
            task_node.ds_worker_group = job_definition.worker_group
            # t_ds_task_definition.environmentCode
            ds_environment_code = ENVIRONMENTS.get(job_definition.environment)
            if ds_environment_code is None:
                print_and_log("配置错误", "作业[{}.{}]的环境[{}]不存在".format(job_definition.project_name, job_definition.name, job_definition.environment))
                raise ValueError("配置错误, 已中止.")

            task_node.ds_environment = job_definition.environment

            # day : today | last1Days
            # month : lastMonthBegin | lastMonthEnd
            # week : lastMonday

            schedule_group = job_definition.group
            last_node = task_node

            desc = job_definition.desc
            group_desc = ""

            if job_definition.is_full_path():
                process_node = tree_nodes.get(config.measure_job_path(job_definition))
                if process_node is None:
                    print_and_log("配置错误", f"作业[{job_definition.project_name}/{job_definition.path}/{job_definition.name}]的路径配置不正确, 使用路径导入时需提前建立对应的分组路径")
                    raise ValueError("配置错误, 已中止.")
                if last_node_is_new:
                    process_node.children.append(last_node)
                    last_node.parent = process_node
            else:
                while True:
                    group_relations = config.project_group_relations.get(job_definition.project_name)
                    if group_relations is None:
                        print_and_log("配置错误", "作业[{}.{}]未定义项目的分组规则".format(job_definition.project_name, job_definition.name))
                        raise ValueError("配置错误, 已中止.")
                    group_relation = group_relations.get(schedule_group)

                    if not group_relation:
                        # 根节点为项目节点
                        if last_node_is_new:
                            project_node.children.append(last_node)
                        break

                    process_name = group_relation.get_name_by_job(job_definition)
                    process_description = group_relation.get_desc_by_job(job_definition)
                    if schedule_group not in ("CVT", "SDB"):
                        group_desc = process_description
                    path = config.measure_job_path(job_definition, schedule_group)

                    process_node = tree_nodes.get(path)
                    if not process_node:
                        process_node = DAGNode('process')
                        process_node.name = process_name
                        #process_node.type = "process"
                        process_node.parent = project_node
                        process_node.description = group_desc
                        process_node.path = path
                        process_node.flag = 'YES'
                        process_node.ds_project_code = project_node.ds_project_code
                        process_node.project_name = project_node.name
                        tree_nodes[process_node.path] = process_node
                        if last_node_is_new:
                            process_node.children.append(last_node)
                            last_node.parent = process_node
                        last_node_is_new = True
                    else:
                        if last_node_is_new:
                            process_node.children.append(last_node)
                            last_node.parent = process_node
                        last_node_is_new = False

                        if group_desc:
                            process_node.description = group_desc
                            group_desc = ""
                    last_node = process_node

                    schedule_group = group_relation.enclosing_group

    # 建立节点[每日]依赖关系, 组成DAG树
    for dependency in config.dependencies:
        
        if '/' in dependency.name:
            dependent_path = f'/{dependency.project}/{dependency.name}'
            orig_task_node = tree_nodes.get(dependent_path)
            if not orig_task_node:
                print_and_log("警告", f"依赖节点 [{dependent_path}] 不存在")
                continue
        else:

            (name, theme, sub_theme, task_name) = config.standardizer.standardize_full(
                dependency.group, dependency.name
            )

            orig_task_node = tree_nodes.get(
                config.measure_job_name_path(dependency.project, dependency.group, name, theme, 'D', sub_theme) + '/' + task_name
            )
            if not orig_task_node:
                orig_task_node = tree_nodes.get(
                    config.measure_job_name_path(dependency.project, dependency.group, name, theme, 'M', sub_theme) + '/' + task_name
                )
            if not orig_task_node:
                orig_task_node = tree_nodes.get(
                    config.measure_job_name_path(dependency.project, dependency.group, name, theme, 'Q', sub_theme) + '/' + task_name
                )
            if not orig_task_node:
                print_and_log("警告", "依赖节点 [{}] 不存在".format(task_name))
                continue

        if '/' in dependency.dependency_name:
            dependency_path = f'/{dependency.dependency_project}/{dependency.dependency_name}'
            orig_dependency_task_node = tree_nodes.get(dependency_path)
        else:
            (
                dependency_name,
                dependency_theme,
                dependency_sub_theme,
                dependency_task_name,
            ) = config.standardizer.standardize_full(
                dependency.dependency_group, dependency.dependency_name
            )

            orig_dependency_task_node = tree_nodes.get(
                config.measure_job_name_path(dependency.dependency_project, dependency.dependency_group, dependency_name, dependency_theme, 'D', dependency_sub_theme) + '/' + dependency_task_name
            )
            if not orig_dependency_task_node:
                orig_dependency_task_node = tree_nodes.get(
                    config.measure_job_name_path(dependency.dependency_project, dependency.dependency_group, dependency_name, dependency_theme, 'M', dependency_sub_theme) + '/' + dependency_task_name
                )
            if not orig_dependency_task_node:
                orig_dependency_task_node = tree_nodes.get(
                    config.measure_job_name_path(dependency.dependency_project, dependency.dependency_group, dependency_name, dependency_theme, 'Q', dependency_sub_theme) + '/' + dependency_task_name
                )

        if not orig_dependency_task_node:
            print_and_log("警告", "被依赖节点 [{}] 不存在".format(dependency_task_name))
            continue

        # 节点路径因增加项目名称, 为保持配置规则不变, 节点层级应-1
        t_level = orig_task_node.path.count("/") - 1
        d_level = orig_dependency_task_node.path.count("/") - 1

        task_node = orig_task_node
        dependency_task_node = orig_dependency_task_node

        while t_level > d_level:
            task_node = task_node.parent
            t_level -= 1

        while t_level < d_level:
            dependency_task_node = dependency_task_node.parent
            d_level -= 1

        # 共同组的级别必须大于 highest_dependent_level, 避免大组间的依赖可能导致的整个流程瘫痪
        # 依赖级别搜索顺序:
        # 1.依赖定义
        # 2.作业定义
        # 分组定义强制限制最高依赖级别
        highest_dependent_level = dependency.highest_dependent_level
        if highest_dependent_level == 0 or highest_dependent_level is None:
            highest_dependent_level = dependency_task_node.highest_dependent_level
        
        if highest_dependent_level is None:
            highest_dependent_level = 0
        
        group_highest_dependent_levels = config.group_highest_dependent_levels.get(dependency_task_node.project_name)
        if group_highest_dependent_levels is None:
            print_and_log(
                "error",
                f'未找到[{dependency_task_node.project_name}/{dependency_task_node.group_code}/{dependency_task_node.name}]的分组被依赖节点的最高依赖级别定义, 需补充配置[分组最高被依赖级别]'
            )
            exit(-1)
        group_highest_dependent_level = group_highest_dependent_levels.get(dependency_task_node.group_code)
        if group_highest_dependent_level is None:
            group_highest_dependent_level = group_highest_dependent_levels.get('其它')
        
        if group_highest_dependent_level is None:
            print_and_log(
                "error",
                f'未找到 [{dependency_task_node.project_name}/{dependency_task_node.group_code}] 或 [{dependency_task_node.project_name}.其它] 的分组被依赖节点的最高依赖级别定义, 需补充配置[分组最高被依赖级别]'
            )
            exit(-1)

        if group_highest_dependent_level > highest_dependent_level:
            highest_dependent_level = group_highest_dependent_level
            dependency_task_node.highest_dependent_level = highest_dependent_level
        
        while t_level >= highest_dependent_level:  # and t_level >= d_level:
            if task_node.parent == dependency_task_node.parent:
                # 同一组下使用DAG依赖
                if dependency_task_node not in task_node.prev_nodes:
                    # 添加依赖关系的节点如果已有自依赖, 将关系添加到自依赖节点上(与自动添加自依赖节点的操作同步,与现有调度已建立的关系同规则)
                    for check_dep_node in task_node.prev_nodes:
                        if check_dep_node.type == 'self_dependent':
                            check_dep_node.prev_nodes.append(dependency_task_node)
                            dependency_task_node.next_nodes.append(check_dep_node)
                            dependency_task_node.dependents.add(check_dep_node)
                            break
                    else:
                        task_node.prev_nodes.append(dependency_task_node)
                        dependency_task_node.next_nodes.append(task_node)
                        dependency_task_node.dependents.add(task_node)
                break
            task_node = task_node.parent
            dependency_task_node = dependency_task_node.parent
            t_level -= 1
            d_level -= 1
        else:
            # 建立依赖节点
            path = orig_task_node.parent.path + "/" + dependency_task_name
            dep_node = tree_nodes.get(path)
            if not dep_node:
                dep_node = DAGNode('dependent')
                tree_nodes[path] = dep_node
                dep_node.name = orig_dependency_task_node.name
                # dep_node.type = "dependent"
                dep_node.dependency_node = orig_dependency_task_node
                dep_node.path = path
                dep_node.cycle = "day"
                dep_node.continuous_check_date = "today"
                dep_node.parent = orig_task_node.parent
                dep_node.parent.children.append(dep_node)
                dep_node.dependents.add(orig_task_node)
                dep_node.description = orig_dependency_task_node.description
                dep_node.flag = 'YES'
                dep_node.project_name = orig_task_node.project_name
                dep_node.ds_project_code = orig_task_node.ds_project_code
                orig_dependency_task_node.dependents.add(dep_node)

            orig_task_node.prev_nodes.append(dep_node)
            dep_node.next_nodes.append(orig_task_node)

    # 自依赖,加到 SELF_DEPENDENT_GROUPS 每个作业流的头部任务之前
    for task_node in [node for node in tree_nodes.values()]:
        if task_node.type in ("process", "self_dependent", "dependent"):
            continue
        if task_node.name == "root":
            continue
        # 无周期定义的任务不增加自依赖
        # if not task_node.cycle:
        #     continue

        if not task_node.continuous_check_date:
            continue

        # self_dep_level = SELF_DEPENDENT_GROUPS[task_node.group_code]
        continuous_check_date = task_node.continuous_check_date

        path = (
            task_node.parent.path + "/" + task_node.name + "_" + continuous_check_date
        )
        cycle_dep_node = tree_nodes.get(path)
        if cycle_dep_node:
            assert task_node.description is not None, repr(task_node)
            cycle_dep_node.description = (
                task_node.description + "_" + continuous_check_date
            )
            continue
            # raise ValueError('path :'+path)
        cycle_dep_node = DAGNode('self_dependent')
        tree_nodes[path] = cycle_dep_node

        # if self_dep_level == 'group':
        #     task_node = task_node.parent

        cycle_dep_node.type = "self_dependent"
        cycle_dep_node.flag = "YES"
        cycle_dep_node.dependency_node = task_node
        cycle_dep_node.path = path
        cycle_dep_node.name = task_node.name + "_" + continuous_check_date
        cycle_dep_node.cycle = task_node.cycle
        cycle_dep_node.continuous_check_date = continuous_check_date
        cycle_dep_node.parent = task_node.parent
        cycle_dep_node.description = task_node.description + "_" + continuous_check_date
        cycle_dep_node.ds_project_code = task_node.ds_project_code
        cycle_dep_node.project_name = task_node.project_name

        cycle_dep_node.prev_nodes = task_node.prev_nodes

        for prev_node in cycle_dep_node.prev_nodes:
            prev_node.next_nodes.remove(task_node)
            prev_node.next_nodes.append(cycle_dep_node)

        task_node.prev_nodes = [cycle_dep_node]
        cycle_dep_node.next_nodes.append(task_node)
        task_node.parent.children.append(cycle_dep_node)

    # return root_node


def load_server_definitions_to_dag(
    project_names: list[str],
    tree_nodes: dict[str, DAGNode],
    config: ScheduleConfig,
    origin_projects: dict[int, dict]
):

    code_process_node_mapping: dict[int, dict[int, DAGNode]] = {}
    code_task_node_mapping: dict[int, dict[int, DAGNode]] = {}
    sub_process_code_mapping: dict[int, int] = {}
    project_code_name_mapping: dict[int, str] = {}

    print_and_log(None, "从服务器加载项目定义:[{}] ...".format(','.join(project_names)), flush=True)

    """根据服务中的定义重新构建DAG"""
    for project_name in project_names:
        project_code, processes, process_schedules = load_remote(project_name)

        if not project_code:
            result = ds_api.create_project(project_name, "")
            project_code = result["code"]
            project_code, processes, process_schedules = load_remote(project_name)
            print_and_log(None, "自动建立项目:[{}][{}]".format(project_name, project_code))
        
        origin_projects[project_code] = { 'name': project_name, 'processes': processes, 'schedules': process_schedules }
        # origin_processes.update(processes)
        project_code_name_mapping[project_code] = project_name
        code_process_node_mapping[project_code] = {}
        code_task_node_mapping[project_code] = {}


        # 生成code和DAGNode的映射
        for process_definition in processes:
            process_obj: dict = process_definition["processDefinition"]
            process_node = DAGNode("process", process_obj["name"])
            process_code = process_obj["code"]
            process_node.ds_process_code = process_code
            process_node.ds_project_code = process_obj['projectCode']
            process_node.project_name = origin_projects[process_node.ds_project_code]['name']
            code_process_node_mapping[process_node.ds_project_code][process_code] = process_node
            tasks: list = process_definition["taskDefinitionList"]
            for task_obj in tasks:
                task_code = task_obj["code"]
                if task_obj["taskType"] == "SHELL":
                    task_node = DAGNode("shell", task_obj["name"])
                    task_node.command = task_obj["taskParams"]["rawScript"]
                elif task_obj["taskType"] == "DEPENDENT":
                    # TODO : 还未支持多依赖处理
                    depend_item = task_obj["taskParams"]["dependence"]["dependTaskList"][0][
                        "dependItemList"
                    ][0]
                    task_node = DAGNode("dependent", task_obj["name"])
                    task_node.continuous_check_date = depend_item["dateValue"]
                    task_node.cycle = depend_item["cycle"]
                else:
                    continue
                # task_node.group_code = task_node.name[:3]
                assert task_obj['projectCode'] == project_code
                task_node.ds_project_code = project_code
                task_node.ds_task_code = task_code
                task_node.description = task_obj["description"]
                if task_node.description is None:
                    task_node.description = '#'
                process_node.children.append(task_node)
                task_node.parent = process_node
                task_node.fail_retry_times = task_obj['failRetryTimes']
                task_node.fail_retry_interval = task_obj['failRetryInterval']
                task_node.flag = task_obj['flag']
                task_node.project_name = origin_projects[task_node.ds_project_code]['name']
                
                # t_ds_task_definition.workerGroup
                task_node.ds_worker_group = task_obj['workerGroup']
                # t_ds_task_definition.environmentCode
                task_node.ds_environment = ENVIRONMENT_NAMES.get(task_obj['environmentCode'])
                if task_node.ds_environment is None:
                    print('任务{}的环境代码{}不存在.'.format(task_obj['name'], task_obj['environmentCode']))

                code_task_node_mapping[project_code][task_code] = task_node

    # 建立DAGNode的DAG关系
    for project_code, origin_project in origin_projects.items():
        processes = origin_project['processes']

        for process_definition in processes:
            process_obj: dict = process_definition["processDefinition"]

            process_node = code_process_node_mapping[project_code][process_obj["code"]]
            tasks: list = process_definition["taskDefinitionList"]
            for task_obj in tasks:
                if task_obj["taskType"] == "SUB_PROCESS":
                    sub_process_code = task_obj["taskParams"]["processDefinitionCode"]
                    try:
                        child_node = code_process_node_mapping[project_code][sub_process_code]
                    except Exception as e:
                        print('子任务不存在{}:{}, 可能 t_ds_relation 中的数据有错误.'.format(task_obj['name'],sub_process_code))
                        continue
                    sub_process_code_mapping[task_obj["code"]] = sub_process_code
                    process_node.children.append(child_node)
                    child_node.parent = process_node
                    child_node.ds_task_name = task_obj["name"]
                    child_node.ds_task_code = task_obj["code"]
                    child_node.flag = task_obj["flag"]
                    
                    # t_ds_task_definition.workerGroup
                    child_node.ds_worker_group = task_obj['workerGroup']
                    # t_ds_task_definition.environmentCode
                    child_node.ds_environment = ENVIRONMENT_NAMES.get(task_obj['environmentCode'])
                    if child_node.ds_environment is None:
                        print('任务{}的环境代码{}不存在.'.format(task_obj['name'], task_obj['environmentCode']))

                elif task_obj["taskType"] == "DEPENDENT":
                    depend_item = task_obj["taskParams"]["dependence"]["dependTaskList"][0][
                        "dependItemList"
                    ][0]
                    dependent_project_code = depend_item['projectCode']
                    dependent_process_code = depend_item['definitionCode']
                    dependent_task_code = depend_item["depTaskCode"]
                    task_code = task_obj["code"]
                    task_node = code_task_node_mapping[project_code][task_code]

                    for mapping_project_codes in code_process_node_mapping.keys():
                        if dependent_task_code == 0:
                            dependency_node = code_process_node_mapping[mapping_project_codes].get(dependent_process_code)
                        else:
                            dependency_node = code_task_node_mapping[mapping_project_codes].get(dependent_task_code)
                        
                        if dependency_node is not None:
                            break
                    
                    assert dependency_node is not None, f'被依赖的节点未找到dependent_task_code[{dependent_task_code}], dependent_process_code[{dependent_process_code}]'
                    # 是否为自依赖需要解析relation之后转换
                    task_node.dependency_node = dependency_node

                    dependency_node.dependents.add(task_node)
                    
                    # t_ds_task_definition.workerGroup
                    task_node.ds_worker_group = task_obj['workerGroup']
                    # t_ds_task_definition.environmentCode
                    task_node.ds_environment = ENVIRONMENT_NAMES.get(task_obj['environmentCode'])
                    if task_node.ds_environment is None:
                        print('任务{}的环境代码{}不存在.'.format(task_obj['name'], task_obj['environmentCode']))

            relations: list = process_definition["processTaskRelationList"]
            for relation_obj in relations:
                pre_task_code = relation_obj["preTaskCode"]
                if pre_task_code == 0:
                    continue

                pre_process_code = sub_process_code_mapping.get(pre_task_code)
                if pre_process_code:
                    pre_node = code_process_node_mapping[project_code][pre_process_code]
                else:
                    # pre_node = code_task_node_mapping[project_code][pre_task_code]
                    pre_node = code_task_node_mapping[project_code].get(pre_task_code)
                    if pre_node is None:
                        continue

                post_task_code = relation_obj["postTaskCode"]
                post_process_code = sub_process_code_mapping.get(post_task_code)
                if post_process_code:
                    post_node = code_process_node_mapping[project_code][post_process_code]
                else:
                    post_node = code_task_node_mapping[project_code].get(post_task_code)
                    # post_node = code_task_node_mapping[project_code][post_task_code]
                    if post_node is None:
                        continue

                pre_node.next_nodes.append(post_node)
                post_node.prev_nodes.append(pre_node)

    # 建立node列表
    root_node = tree_nodes["root"]
    for project_code, project_obj in origin_projects.items():
        project_name = project_obj['name']
        project_node = DAGNode('project', project_name)
        project_node.project_name = project_name
        project_node.ds_project_code = project_code
        project_node.parent = root_node
        project_node.project_modifiable = (project_name in config.modifiable_projects)
        root_node.children.append(project_node)
        project_node.measure_path()

        tree_nodes[project_node.path] = project_node

        process_node_mapping = code_process_node_mapping[project_code]

        exclude_names = config.exclude_names.get(project_name, set())

        for node in process_node_mapping.values():
            if not node.parent:
                project_node.children.append(node)
                node.parent = project_node

        for node in process_node_mapping.values():
            assert node.project_name == project_name
            path = node.measure_path()
            for exclude_path in exclude_names:
                if path.startswith(f'/{node.project_name}{exclude_path}'):
                    node.ignored = True
                    break
            
            assert tree_nodes.get(path) is None
            tree_nodes[path] = node

        task_node_mapping = code_task_node_mapping[project_code]
        for node in task_node_mapping.values():
            path = node.measure_path()
            for exclude_path in exclude_names:
                if path.startswith(f'/{node.project_name}{exclude_path}'):
                    node.ignored = True
                    break

            assert tree_nodes.get(path) is None
            tree_nodes[path] = node
            
            if node.ignored:
                continue

            head_path = path[len(node.project_name) + 1: ]
            
            if not head_path.startswith("/HEAD_"):
                # raise ValueError()
                node.cycle = "day"
            else:
                for k, v in config.cycles.items():
                    if head_path.startswith(k, 6):
                        node.cycle = v.cycle_name
                        break
                else:
                    raise ValueError(head_path)

            # TODO : 以下规则仅适用于自动生成的节点
            if node.type == "dependent":
                if (
                    node.continuous_check_date.startswith("last")
                    and len(node.next_nodes) == 1
                    and node.dependency_node in node.next_nodes
                ):
                    # 自依赖节点, 周期与作业周期有关
                    node.type = "self_dependent"
                    node.dependency_node.continuous_check_date = node.continuous_check_date
                else:
                    # 非自依赖节点, 只检查当天批次内的作业
                    node.cycle = "day"

    print_and_log(
        None, "导入共计 [{}] 个节点.".format(len(tree_nodes))
    )

def load_remote(project_name: str):

    project_code = ds_api.get_project_code_by_name(project_name)

    if project_code == 0:
        return (None, None, None)

    process_def_list = ds_api.get_process_definitions_by_project_code(project_code)
    process_schedule_list = ds_api.get_schedules_by_project_code(project_code)
    return (project_code, process_def_list, process_schedule_list)


def import_to_server(
    root_node: DAGNode,
    origin_projects: dict[int, dict],
    tenant_name: str,
    # environment_name: str,
    confirm_method,
    is_generate_void: bool = False,
):
    """
    * 数据仓库一般作业量会达到2000以上, 而单个作业不能直接按子任务分大组依赖, 因上游的不确定性容易造成大面积堵塞,
    * 因此作业加工之间的依赖在小组内只能靠依赖节点

    * 因为数仓任务涉及业务连续性问题, 不允许跨日执行, 大部分作业节点必须有前一天的任务自依赖,
    * 有含依赖节点的流程被更新或建立时, 需要同时生成上一天的成功的任务实例
    * 直接在`数据库`生成成功的任务实例

    * 由于dependent任务和sub_process任务的参数中,指向其他作业的code在导入时无法自动映射,
    * 故必须使用api获取已存在的流程或提前生成有效的空流程, 并直接将实际存在的流程code引用到参数中.
    * 作业生成流程:
        1. 遍历 process_definition 复原任务 DAG
        2. 遍历 增量配置 在已有 DAG 的基础上增量更新, 必须保证 '流程名称.任务名称' 唯一
        3. 遍历 DAG 生成流程(新增流程及任务 code=0), 与项目下原有的流程比对, 标记需要新增或修改的流程
        4. 新增的流程建立空流程, 生成code后再写入流程
    """
    if root_node.type != "root":
        raise ValueError("must be root node to call this method.")

    # environment_code = ds_api.get_environment_code_by_name(environment_name)
    processes: dict[int, dict[int, dict]] = {}
    tasks: dict[str, dict[int, dict]] = {}
    code_node_mapping: dict[str, dict[int, dict]] = {}
    schedules: dict[int, dict[int, dict]] = {}

    # 映射项目下的所有对象
    for project_code, project_def in origin_projects.items():
        processes[project_code] = {}
        tasks[project_code] = {}
        code_node_mapping[project_code] = {}
        schedules[project_code] = {}
        project_processes = project_def['processes']
        for process_def in project_processes:
            processes[project_code][process_def["processDefinition"]["code"]] = process_def
            for task_def in process_def["taskDefinitionList"]:
                tasks[project_code][task_def['code']] = task_def

        for process_schedules in project_def['schedules']:
            schedules[project_code][process_schedules['processDefinitionCode']] = process_schedules

    params = AutoParams(
        {
            # 'task_code': task_codes,
            "origin_projects": origin_projects,
            "environments": ENVIRONMENTS,
            "tenant_name": tenant_name,
            "old_process": processes,
            "old_tasks": tasks,
            'schedules': schedules,
            "is_generate_void": is_generate_void,
            "task_codes": [],
            "changed_process_count": 0,
            "init_process": None,
            "operation_time": LOG_TIME_STR,
            'code_node_mapping': code_node_mapping,
        }
    )

    for child_node in root_node.children:
        child_node.match_processes_and_codes(params)

    print_and_log(None, "重新生成定义 ...")
    for child_node in root_node.children:
        child_node.gen_ds_node(params)

    print_and_log(None, "比较差异 ...")
    for child_node in root_node.children:
        child_node.compare_changes(params)

    # 记录未覆盖到的流程(不删除)
    for project_code, processes_in_project in processes.items():
        project_name = origin_projects[project_code]['name']
        if processes_in_project:
            for process in processes_in_project.values():
                print_and_log(
                    "not_covered",
                    "此次操作未覆盖到流程:[{}]/[{}][{}]".format(project_name, process['processDefinition']["name"], process['processDefinition']["code"]),
                )

    if params.get("changed_process_count"):
        if not confirm_method("是否确认以上变更?", "已确认.", "已取消."):
            return False

    if params.get("changed_process_count"):
        # 用流程名称匹配项目下已有的流程code, 存在时从old_process列表移出, 不存在时建立process.
        # 遍历每个任务节点领取通过api提前生成的任务code
        print_and_log(None, "生成新增对象标识 ...")
        for child_node in root_node.children:
            child_node.create_processes_and_codes(params)

        print_and_log(None, "重新生成定义 ...")
        for child_node in root_node.children:
            child_node.gen_ds_node(params)

        print_and_log(None, "合并到服务器 ...")
        for child_node in root_node.children:
            child_node.merge_ds_node(params)

        if confirm_method("是否建立伪实例(有新增依赖上日任务节点必须建立伪实例)?", "已确认.", "已取消."):
            print_and_log(None, "建立伪实例 ...")
            connection = ds_db.get_db_connection()

            schedule_time = LOG_TIME_STR[:11] + "00:00:00"

            biz_date = datetime.strptime(LOG_TIME_STR[:10], "%Y-%m-%d") - timedelta(
                days=1
            )

            instance_params = AutoParams(
                {
                    "connection": connection,
                    "schedule_time": schedule_time,
                    "start_time": LOG_TIME_STR,
                    "end_time": LOG_TIME_STR,
                    "host": "fake_host",
                    "biz_date": biz_date.strftime("%Y%m%d"),
                    "executor_id": 2,
                    "environments": ENVIRONMENTS,
                    # "environment_code": environment_code,
                }
            )

            for child_node in root_node.children:
                child_node.create_fake_instance(instance_params)

            connection.close()

        print_and_log(None, "导入完成.")

        return True
    else:
        print_and_log(None, "未对此项目产生变更!")
        return False


def import_from_xlsx(
    tennat_name, xlsx_path: str, confirm_method=confirm
):

    config = ScheduleConfig.from_xlsx(xlsx_path)

    # 检查配置覆盖范围
    for job_definition in config.jobs.values():
        PROJECT_NAMES.add(job_definition.project_name)
    for dependency in config.dependencies:
        PROJECT_NAMES.add(dependency.project)
        PROJECT_NAMES.add(dependency.dependency_project)
    
    print_and_log(None, f"配置所覆盖的项目:[{','.join(PROJECT_NAMES)}], 配置可操作的项目:[{','.join(config.modifiable_projects)}]")

    root_node = DAGNode("root", '')
    tree_nodes: dict[str, DAGNode] = {"root": root_node}
    origin_projects: dict[int, dict] = {}

    load_server_definitions_to_dag(PROJECT_NAMES, tree_nodes, config, origin_projects)

    # else:
    #     result = ds_api.create_project(project_name, "")
    #     project_code = result["code"]
    #     print_and_log(None, "建立项目:[{}][{}]".format(project_name, project_code))

    print_and_log(None, "加载配置:[{}] ...".format(xlsx_path))
    load_config_to_dag(tree_nodes, config, origin_projects)
    print_and_log(
        None, "导入 [{}] 个任务和 [{}] 个依赖".format(len(config.jobs), len(config.dependencies))
    )

    import_to_server(
        root_node, origin_projects, tennat_name, confirm_method
    )


if __name__ == "__main__":
    

    origin_environments = ds_api.get_environments()

    for environment in origin_environments:
        ENVIRONMENTS[environment['name']] = environment['code']
        ENVIRONMENT_NAMES[environment['code']] = environment['name']
    ENVIRONMENT_NAMES[0] = 'default'
    ENVIRONMENTS['default'] = 0
    
    if len(sys.argv) == 1:
        config_xlsx_path = input('\n输入调度配置路径：')
        import_from_xlsx(
            "etl", config_xlsx_path
        )
    else:
        import_from_xlsx(
            "etl", sys.argv[1]
        )
