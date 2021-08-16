# process_definition_import_tool
Dolphinscheduler 的一个外部作业导入工具

 - [用途](#用途)
 - [代码概述](#代码概述)
 - [特性](#特性)
 - [生成过程](#生成过程)


---

## 用途
将通用的外部定义的作业配置导入到 Dolphinscheduler 指定的项目中


## 代码概述

* `general_scripts.py`
  通用配置定义
  * `etl_jobs`
    基础作业任务，前几个关键元素分别是 `分组`,`主题`,`作业名`,`执行周期`,`周期偏移量`
  * `etl_dependencies`
    基础作业间的依赖关系, 元素分别是 `分组`,`主题`,`作业名`,`依赖的分组`,`依赖的主题`,`依赖的作业名`

* `etl_node.py`
  定义了一个树形对象, 用于存放`general_scripts.py`中定义的作业, `gen_ds_node` 实现了一个转化为Dolphinscheduler对象(相当于实例化的json)

* `ds_generator.py`
  主要实现Dolphinscheduler对象的生成方法

* `ds_config.py`
  ds所用的数据库配置及要导入的项目配置(项目名称/租户名称/用户名称)

* `ds_db.py`
  用于ds的数据库操作 db_update_max_id 用于更新t_ds_process_definition的当前自增id(1.3.6的id是数据库自动生成，外部导入后必须更新，否则手动添加流程报错)

* `main_general.py`
  将general_scripts.py的定义转化为`etl_node.py`中的`EtlNode`对象,并生成Dolphinscheduler的对象,转换为t_ds_process_definition，导入到数据库。
  * `SELF_DEPENDENT_CYCLE_MAPPINGS` 定义作业定义中 `执行周期`+`周期偏移量` 与 `DS自依赖周期`,`DS自依赖日期`,`DS周期组` 的关系(用于自动生成数仓任务中的自依赖节点)
  * `SELF_DEPENDENT_GROUPS` 定义哪些`分组`中的任务需要添加`自依赖`节点
  * `PROCESS_RELATIONS` 定义如何对`general_scripts.py`中的作业进行`多层分组`
    * 多层分组后,会通过`子流程`的方式关联
    * 元素为: `分组key`,[`上级分组key`,`上级分组流程名称`,`上级分组流程描述`],(生成时根据`上级分组key`循环生成上级分组,直至没有上级分组)


## 特性

* `main_general.py`.`SELF_DEPENDENT_CYCLE_MAPPINGS` 目前通过 `分组`,`主题`,`周期` 来生成对应的分组
* `EtlNode`.`path` 用来标识任务节点的实际位置
* `t_ds_process_definition` 的更新过程是增量的,先获取到表中的最大id,生成数据后通过`t_ds_process_definition`.`name`查找已存在的`id`,未找到的记录自动生成新`id`,并`删除`项目中不在生成结果集中的记录
* `3级以下`分组的`任务的依赖`会上升为同级别(3级以下)中`子任务间的依赖`,`3级以下`分组找不到共同分组时,自动添加任务的`依赖节点`, [逻辑在这里](https://github.com/reele/process_definition_import_tool/blob/e34a568248432442337a72613dd3173fd581b14a/main_general.py#L237)


## 生成过程

1. DS中添加`租户`,`用户`和`项目`
2. 配置对应的`ds_config.py`
3. 生成`general_scripts.py`
4. 对照`general_scripts.py`, 编辑`main_general.py`中的`SELF_DEPENDENT_CYCLE_MAPPINGS`,`SELF_DEPENDENT_GROUPS`,`PROCESS_RELATIONS`
