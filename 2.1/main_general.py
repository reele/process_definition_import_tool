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
):
    root_node = tree_nodes["root"]

    task_cycle_mappings = {}

    # 生成任务节点, 建立按分组关系建立分组节点
    for job_full_name, job_definition in config.jobs.items():
        # error
        # if job_definition.origin_job_name == 'T03_AGMT_AMT_H_S07_2' and job_definition.scriptname == 't03_agmt_amt_h_s07_30200.pl':
        #     continue

        task_name = job_definition.full_name

        shell_command = job_definition.raw_script

        cycle = job_definition.cycle
        task_cycle_mappings[task_name] = cycle
        path = config.get_job_path(job_definition) + "/" + task_name

        task_node = tree_nodes.get(path)
        if task_node:
            if task_node.type != "shell":
                raise ValueError("作业名称与组名称冲突")
            task_node.ds_modify_desc = "update"
            last_node_is_new = False
        else:
            task_node = DAGNode()
            task_node.ds_modify_desc = "create"
            tree_nodes[path] = task_node
            last_node_is_new = True

        task_node.name = task_name
        task_node.type = "shell"
        task_node.command = shell_command
        task_node.description = job_definition.desc
        task_node.path = path
        task_node.cycle = job_definition.cycle.cycle_name
        task_node.ds_task_name = None  # 若节点已存在, 覆盖名称
        task_node.fail_retry_times = job_definition.fail_retry_times
        task_node.fail_retry_interval = job_definition.fail_retry_interval

        if not job_definition.self_dependent and task_node.continuous_check_date:
            self_dependent_node = tree_nodes.get(
                config.get_job_path(job_definition) + "/" + task_name + "_" + task_node.continuous_check_date
            )
            self_dependent_node.parent.children.remove(self_dependent_node)
            for prev_node in self_dependent_node.prev_nodes:
                prev_node.next_nodes.remove(self_dependent_node)
                prev_node.next_nodes.append(task_node)
            task_node.prev_nodes = self_dependent_node.prev_nodes
            task_node.continuous_check_date = None
            tree_nodes.pop(self_dependent_node.path)
        elif job_definition.self_dependent and not task_node.continuous_check_date:
            task_node.continuous_check_date = (
                job_definition.cycle.self_dependent_type
                if job_definition.self_dependent
                else None
            )

        task_node.group_code = job_definition.group
        task_node.max_dag_dep_level = job_definition.max_dependent_level

        # day : today | last1Days
        # month : lastMonthBegin | lastMonthEnd
        # week : lastMonday

        schedule_group = job_definition.group
        last_node = task_node

        desc = job_definition.desc
        group_desc = ""

        while True:
            group_relation = config.group_relations.get(schedule_group)

            if not group_relation:
                if last_node_is_new:
                    root_node.children.append(last_node)
                break

            process_name = group_relation.get_name_by_job(job_definition)
            process_description = group_relation.get_desc_by_job(job_definition)
            if schedule_group not in ("CVT", "SDB"):
                group_desc = process_description
            path = config.get_job_path(job_definition, schedule_group)

            process_node = tree_nodes.get(path)
            if not process_node:
                process_node = DAGNode()
                process_node.name = process_name
                process_node.type = "process"
                process_node.parent = root_node
                process_node.description = group_desc
                process_node.path = path
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
        (name, theme, sub_theme, task_name) = config.standardizer.standardize_full(
            dependency.group, dependency.name
        )

        (
            dependency_name,
            dependency_theme,
            dependency_sub_theme,
            dependency_task_name,
        ) = config.standardizer.standardize_full(
            dependency.dependency_group, dependency.dependency_name
        )

        orig_task_node = tree_nodes.get(
            config.get_job_name_path(dependency.group, name, theme, 'D', sub_theme) + '/' + task_name
        )
        if not orig_task_node:
            print_and_log("警告", "依赖节点 [{}] 不存在".format(task_name))
            continue

        orig_dependency_task_node = tree_nodes.get(
            config.get_job_name_path(dependency.dependency_group, dependency_name, dependency_theme, 'D', dependency_sub_theme) + '/' + dependency_task_name
        )
        if not orig_dependency_task_node:
            print_and_log("警告", "被依赖节点 [{}] 不存在".format(dependency_task_name))
            continue

        t_level = orig_task_node.path.count("/")
        d_level = orig_dependency_task_node.path.count("/")

        task_node = orig_task_node
        dependency_task_node = orig_dependency_task_node

        while t_level > d_level:
            task_node = task_node.parent
            t_level -= 1

        while t_level < d_level:
            dependency_task_node = dependency_task_node.parent
            d_level -= 1

        # 共同组的级别必须大于 max_dag_dep_level, 避免大组间的依赖可能导致的整个流程瘫痪
        max_dag_dep_level = dependency_task_node.max_dag_dep_level
        if max_dag_dep_level == 0 or max_dag_dep_level is None:
            max_dag_dep_level = 3
        while t_level >= max_dag_dep_level:  # and t_level >= d_level:
            if task_node.parent == dependency_task_node.parent:
                # 同一组下使用DAG依赖
                if dependency_task_node not in task_node.prev_nodes:
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
                dep_node = DAGNode()
                tree_nodes[path] = dep_node
                dep_node.name = orig_dependency_task_node.name
                dep_node.type = "dependent"
                dep_node.dependency_node = orig_dependency_task_node
                dep_node.path = path
                dep_node.cycle = "day"
                dep_node.continuous_check_date = "today"
                dep_node.parent = orig_task_node.parent
                dep_node.parent.children.append(dep_node)
                dep_node.dependents.add(orig_task_node)
                dep_node.description = orig_dependency_task_node.description
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
            cycle_dep_node.description = (
                task_node.description + "_" + continuous_check_date
            )
            continue
            # raise ValueError('path :'+path)
        cycle_dep_node = DAGNode()
        tree_nodes[path] = cycle_dep_node

        # if self_dep_level == 'group':
        #     task_node = task_node.parent

        cycle_dep_node.type = "self_dependent"
        cycle_dep_node.dependency_node = task_node
        cycle_dep_node.path = path
        cycle_dep_node.name = task_node.name + "_" + continuous_check_date
        cycle_dep_node.cycle = task_node.cycle
        cycle_dep_node.continuous_check_date = continuous_check_date
        cycle_dep_node.parent = task_node.parent
        cycle_dep_node.description = task_node.description + "_" + continuous_check_date

        cycle_dep_node.prev_nodes = task_node.prev_nodes

        for prev_node in cycle_dep_node.prev_nodes:
            prev_node.next_nodes.remove(task_node)
            prev_node.next_nodes.append(cycle_dep_node)

        task_node.prev_nodes = [cycle_dep_node]
        cycle_dep_node.next_nodes.append(task_node)
        task_node.parent.children.append(cycle_dep_node)

    # return root_node


def load_server_definitions_to_dag(
    tree_nodes: dict[str, DAGNode],
    origin_processes: dict,
    config: ScheduleConfig,
):

    """根据服务中的定义重新构建DAG"""

    code_process_node_mapping: dict[int, DAGNode] = {}
    code_task_node_mapping: dict[int, DAGNode] = {}
    sub_process_code_mapping: dict[int, int] = {}
    dependency_code_mapping: dict[int, int] = {}

    # 生成code和DAGNode的映射
    for process_definition in origin_processes.values():
        process_obj: dict = process_definition["definition"]
        process_node = DAGNode("process", process_obj["name"])
        process_code = process_obj["code"]
        process_node.ds_process_code = process_code
        code_process_node_mapping[process_code] = process_node
        tasks: dict = process_definition["tasks"]
        for task_obj in tasks.values():
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
            task_node.group_code = task_node.name[:3]
            code_task_node_mapping[task_code] = task_node
            task_node.ds_task_code = task_code
            task_node.description = task_obj["description"]
            process_node.children.append(task_node)
            task_node.parent = process_node
            task_node.fail_retry_times = task_obj['failRetryTimes']
            task_node.fail_retry_interval = task_obj['failRetryInterval']

    # 建立DAGNode的DAG关系
    for process_definition in origin_processes.values():
        process_obj: dict = process_definition["definition"]

        process_node = code_process_node_mapping[process_obj["code"]]
        tasks: dict = process_definition["tasks"]
        for task_obj in tasks.values():
            if task_obj["taskType"] == "SUB_PROCESS":
                sub_process_code = task_obj["taskParams"]["processDefinitionCode"]
                try:
                    child_node = code_process_node_mapping[sub_process_code]
                except Exception as e:
                    print('子任务不存在{}:{}'.format(task_obj['name'],sub_process_code))
                    continue
                sub_process_code_mapping[task_obj["code"]] = sub_process_code
                process_node.children.append(child_node)
                child_node.parent = process_node
                child_node.ds_task_name = task_obj["name"]
                child_node.ds_task_code = task_obj["code"]

            elif task_obj["taskType"] == "DEPENDENT":
                depend_item = task_obj["taskParams"]["dependence"]["dependTaskList"][0][
                    "dependItemList"
                ][0]
                dependent_task_code = depend_item["depTaskCode"]
                task_code = task_obj["code"]
                if dependent_task_code == 0:
                    dependency_code_mapping[task_code] = depend_item["definitionCode"]
                    dependency_node = code_process_node_mapping[
                        depend_item["definitionCode"]
                    ]
                else:
                    dependency_code_mapping[task_code] = dependent_task_code
                    dependency_node = code_task_node_mapping[dependent_task_code]
                task_node = code_task_node_mapping[task_code]

                # 是否为自依赖需要解析relation之后转换

                task_node.dependency_node = dependency_node
                dependency_node.dependents.add(task_node)

        relations: dict = process_definition["relations"]
        for relation_obj in relations.values():
            pre_task_code = relation_obj["preTaskCode"]
            if pre_task_code == 0:
                continue

            pre_process_code = sub_process_code_mapping.get(pre_task_code)
            if pre_process_code:
                pre_node = code_process_node_mapping[pre_process_code]
            else:
                pre_node = code_task_node_mapping[pre_task_code]

            post_task_code = relation_obj["postTaskCode"]
            post_process_code = sub_process_code_mapping.get(post_task_code)
            if post_process_code:
                post_node = code_process_node_mapping[post_process_code]
            else:
                post_node = code_task_node_mapping[post_task_code]

            pre_node.next_nodes.append(post_node)
            post_node.prev_nodes.append(pre_node)

    # 建立node列表
    root_node = tree_nodes["root"]
    for node in code_process_node_mapping.values():
        path = node.measure_path()
        try:
            head_name = path[: path.index("/", 1)]
        except ValueError:
            head_name = path

        if head_name in config.exclude_names:
            continue

        tree_nodes[path] = node
        if not node.parent:
            root_node.children.append(node)
            node.parent = root_node

    for node in code_task_node_mapping.values():
        path = node.measure_path()

        head_name = path[: path.index("/", 1)]
        if head_name in config.exclude_names:
            continue
        if not head_name.startswith("/HEAD_"):
            # raise ValueError()
            node.cycle = "day"
        else:
            for k, v in config.cycles.items():
                if head_name.startswith(k, 6):
                    node.cycle = v.cycle_name
                    break
            else:
                raise ValueError(head_name)

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

        tree_nodes[path] = node


def load_remote(project_name: str):

    project_code = ds_api.get_project_code_by_name(project_name)

    if project_code == 0:
        return (None, None)

    process_def_list = ds_api.get_process_definitions_by_project_code(project_code)
    processes = {}

    for process_def in process_def_list:
        process_name = process_def["processDefinition"]["name"]
        task_def_list = process_def["taskDefinitionList"]
        task_relation_list = process_def["processTaskRelationList"]

        process_definition = {"definition": process_def["processDefinition"]}
        processes[process_name] = process_definition

        tasks = {}
        for task_def in task_def_list:
            task_idendifier = "{process_name}:{task_name}:{task_type}".format(
                process_name=process_name,
                task_name=task_def["name"],
                task_type=task_def["taskType"],
            )
            tasks[task_idendifier] = task_def
        process_definition["tasks"] = tasks

        relations = {}
        for task_relation in task_relation_list:
            relation_idendifier = "{pre_task_code}_{post_task_code}".format(
                pre_task_code=task_relation["preTaskCode"],
                post_task_code=task_relation["postTaskCode"],
            )
            relations[relation_idendifier] = task_relation
        process_definition["relations"] = relations

    return (project_code, processes)


def import_to_server(
    root_node: DAGNode,
    project_code: int,
    tenant_name: str,
    environment_name: str,
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

    environment_code = ds_api.get_environment_code_by_name(environment_name)

    origin_processes: dict[str, Any] = {}
    processes: dict[str, int] = {}
    tasks: dict[str, int] = {}

    # 导出项目下的所有流程
    process_def_list = ds_api.get_process_definitions_by_project_code(project_code)

    for process_def in process_def_list:
        process_def_main = process_def["processDefinition"]
        task_def_list = process_def["taskDefinitionList"]
        process_name = process_def_main["name"]
        processes[process_name] = process_def_main
        origin_processes[process_name] = process_def
        for task_def in task_def_list:
            task_idendifier = "{process_name}:{task_name}:{task_type}".format(
                process_name=process_name,
                task_name=task_def["name"],
                task_type=task_def["taskType"],
            )
            tasks[task_idendifier] = task_def

    params = AutoParams(
        {
            # 'task_code': task_codes,
            "project_code": project_code,
            "environment_code": environment_code,
            "tenant_name": tenant_name,
            "old_process": processes,
            "old_tasks": tasks,
            "origin_processes": origin_processes,
            "is_generate_void": is_generate_void,
            "task_codes": [],
            "changed_process_count": 0,
            "init_process": None,
            "operation_time": LOG_TIME_STR,
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
    if processes:
        for process in processes.values():
            # ds_api.delete_process_by_code(
            #     project_code, process['code'], True, process['name'])
            # print('deleted process:{}, code:{}.'.format(
            #     process['name'], process['code']))
            print_and_log(
                "not_covered",
                "此次操作未覆盖到流程:[{}][{}]".format(process["name"], process["code"]),
            )

    if params.get("changed_process_count"):
        if not confirm_method("是否确认以上变更?", "已确认.", "已取消."):
            return False

    if params.get("changed_process_count"):
        # 用流程名称匹配项目下已有的流程code, 存在时从old_process列表移出, 不存在时建立process.
        # 遍历每个任务节点领取通过api提前生成的任务code
        print_and_log(None, "建立新节点 ...")
        for child_node in root_node.children:
            child_node.create_processes_and_codes(params)

        print_and_log(None, "重新生成定义 ...")
        for child_node in root_node.children:
            child_node.gen_ds_node(params)

        print_and_log(None, "合并到服务器 ...")
        for child_node in root_node.children:
            child_node.merge_ds_node(params)

        if confirm_method("是否建立伪实例(有新增节点必须建立伪实例)?", "已确认.", "已取消."):
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
                    "environment_code": environment_code,
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
    project_name, tennat_name, environment_name, xlsx_path: str, confirm_method=confirm
):

    config = ScheduleConfig.from_xlsx(xlsx_path)

    root_node = DAGNode("root", "root")
    tree_nodes: dict[str, DAGNode] = {"root": root_node}

    project_code, origin_processes = load_remote(project_name)
    if project_code:
        print_and_log(None, "从服务器加载项目定义:[{}] ...".format(project_name))
        load_server_definitions_to_dag(tree_nodes, origin_processes, config)
        print_and_log(
            None, "导入共计 [{}] 个节点.".format(len(tree_nodes))
        )
    else:
        result = ds_api.create_project(project_name, "")
        project_code = result["code"]
        print_and_log(None, "建立项目:[{}][{}]".format(project_name, project_code))

    print_and_log(None, "加载配置:[{}] ...".format(xlsx_path))
    load_config_to_dag(tree_nodes, config)
    print_and_log(
        None, "导入 [{}] 个任务和 [{}] 个依赖".format(len(config.jobs), len(config.dependencies))
    )

    import_to_server(
        root_node, project_code, tennat_name, environment_name, confirm_method
    )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        config_xlsx_path = input('\n输入调度配置路径：')
        import_from_xlsx(
            "[dw_main][1.1]", "etl", "etl", config_xlsx_path
        )
    else:
        import_from_xlsx(
            "[dw_main][1.1]", "etl", "etl", sys.argv[1]
        )
