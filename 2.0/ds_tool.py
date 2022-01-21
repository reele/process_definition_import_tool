#!/usr/bin/python3
import sys
from utils import ds_api
from utils import main_general
from optparse import OptionParser


def delete_all_process_by_project_name(project_name: str, is_force_delete: bool):
    project_code = ds_api.get_project_code_by_name(project_name)
    if project_code:
        process_simple_list = ds_api.get_process_simple_list_by_project_code(
            project_code)
        for process in process_simple_list:
            ds_api.delete_process_by_code(
                project_code, process['code'], is_force_delete, process['name'])
            print('deleted process:{} from project {}.'.format(
                process['name'], project_name))

        print(' {} processes deleted.'.format(len(process_simple_list)))
    else:
        print('Project {} not found.'.format(project_name))


def make_all_process_online_by_project_name(project_name: str):
    project_code = ds_api.get_project_code_by_name(project_name)
    if project_code:
        process_simple_list = ds_api.get_process_simple_list_by_project_code(
            project_code)
        for process in process_simple_list:
            ds_api.update_process_state_by_code(
                project_code, process['code'], process['name'], 'ONLINE')
            print('process:{} from project {} is online.'.format(
                process['name'], project_name))

        print(' {} processes are online.'.format(len(process_simple_list)))
    else:
        print('Project {} not found.'.format(project_name))


def make_all_process_offline_by_project_name(project_name: str):
    project_code = ds_api.get_project_code_by_name(project_name)
    if project_code:
        process_simple_list = ds_api.get_process_simple_list_by_project_code(
            project_code)
        for process in process_simple_list:
            ds_api.update_process_state_by_code(
                project_code, process['code'], process['name'], 'OFFLINE')
            print('process:{} from project {} is offline.'.format(
                process['name'], project_name))

        print(' {} processes are offline.'.format(len(process_simple_list)))
    else:
        print('Project {} not found.'.format(project_name))


def merge_task_configurations_to_ds_server(project_name: str, tenant_name: str):
    root_node = main_general.gen_dag()
    root_node.import_to_ds_server(project_name, tenant_name, True)
    input('Press any key after a full void run...')
    root_node.import_to_ds_server(project_name, tenant_name, False)


def get_parser():
    usage = '''usage: python/python3 %prog [command] [options]

DolphinScheduler project control tool

Commands:
   delete:  Delete all processes from project.
  offline:  Change all process's states to offline in project.
   online:  Change all process's states to online in project.
    merge:  merge task configurations to DS server by project name and tenant name.
    '''
    parser = OptionParser(usage=usage)
    parser.add_option("--project_name", action="store", type="string", dest="project_name",
                      help="Delete all process by project name.")
    parser.add_option("--tenant_name", action="store", type="string", dest="tenant_name",
                      help="The process's tenant name.")
    parser.add_option("-f", action="store_true", default=False, dest="force",
                      help="Force operation.")

    return parser


if __name__ == '__main__':
    parser = get_parser()
    if len(sys.argv) <= 2:
        parser.print_help()
        exit(-1)
    
    command = sys.argv[1]
    (options, args) = parser.parse_args(sys.argv[2:])

    if command == 'delete' and options.project_name:
        delete_all_process_by_project_name(options.project_name, options.force)
    elif command == 'offline' and options.project_name:
        make_all_process_offline_by_project_name(options.project_name)
    elif command == 'online' and options.project_name:
        make_all_process_online_by_project_name(options.project_name)
    elif command == 'merge' and options.project_name and options.tenant_name:
        merge_task_configurations_to_ds_server(
            options.project_name, options.tenant_name)
    else:
        parser.print_help()
