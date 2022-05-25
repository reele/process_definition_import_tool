#!/usr/bin/python3
import sys
import ds_api
import main_general
from optparse import OptionParser


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

def delete_all_process_by_project_name(project_name: str, is_force_delete: bool):
    project_code = ds_api.get_project_code_by_name(project_name)
    if project_code:
        process_simple_list = ds_api.get_process_simple_list_by_project_code(
            project_code)
        
        if len(process_simple_list) == 0:
            print('The project [{}] is void.'.format(project_name))
            return

        message = '''=================================================
                  Warning!
  All {} process definitions will be deleted.
  Input [yes] and Enter to continue.
  Or input [no] and Enter to cancel.
=================================================
[yes/no]: '''.format(len(process_simple_list))
        
        if not confirm(message, '', 'Canceled.'):
            return
        
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


def merge_task_configurations_to_ds_server(project_name: str, tenant_name: str, env_name: str, file_name: str):
    root_node = main_general.import_from_xlsx(project_name, tenant_name, env_name, file_name)

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
    parser.add_option("--env_name", action="store", type="string", dest="env_name",
                      help="The process's environment name.")
    parser.add_option("--file", action="store", type="string", dest="file_name",
                      help="Xlsx config file to load.")
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
            options.project_name, options.tenant_name, options.env_name, options.file_name)
    else:
        parser.print_help()
