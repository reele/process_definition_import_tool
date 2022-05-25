#!/usr/bin/python3

import tkinter
import tkinter.ttk
import tkinter.filedialog
import tkinter.messagebox
from tkinter.font import Font
from tkinter import font as tk_font
import simple_log
import main_general
import threading
import inspect
import ctypes

ELEMENTS = {

}

def logout(prefix, message):
    append_text = '{}{}\n'.format(
        '[{}]:'.format(prefix) if prefix else '',
         message
    )
    ELEMENTS['text_log'].insert(tkinter.END, append_text)

def confirm(message, msg_yes='', msg_no=''):
    
    result = False
    def yes():
        nonlocal result
        result = True
        window.destroy()
    def no():
        window.destroy()

    window = tkinter.Tk()
    window.title('确认')
    text = ELEMENTS['text_log'].get(0.0, tkinter.END)
    tk_text = tkinter.Text(window, foreground='blue')
    for line in text.splitlines(True):
        if line.startswith('[cover_change]'):
            tk_text.insert(tkinter.END, line)
    tk_text.grid(row=0, column=0, columnspan=2, padx=5, pady=5)
    tk_message = tkinter.ttk.Label(window, text=message, foreground='red')
    tk_message.grid(row=1, column=0, columnspan=2, padx=5, pady=5)
    yes_button = tkinter.ttk.Button(window, text='确认', command=yes)
    no_button = tkinter.ttk.Button(window, text='取消', command=no)
    yes_button.grid(row=2, column=0, padx=5, pady=5, sticky=tkinter.E)
    no_button.grid(row=2, column=1, padx=5, pady=5, sticky=tkinter.W)
    window.wait_window()

    logout(message, msg_yes if result else msg_no)

    return result



def select_file():
    filename = tkinter.filedialog.askopenfilename(defaultextension='xlsx', filetypes=(('xlsx', ''), ('xls', '')))
    if filename:
        ELEMENTS['input_file_name'].delete(0, tkinter.END)
        ELEMENTS['input_file_name'].insert(0, filename)

def import_file():
    try:
        ELEMENTS['button_import']['state'] = tkinter.DISABLED
        ELEMENTS['text_log'].delete(0.0, tkinter.END)
        main_general.import_from_xlsx(
            ELEMENTS['input_project_name'].get(),
            ELEMENTS['input_tenant_name'].get(),
            ELEMENTS['input_env_name'].get(),
            ELEMENTS['input_file_name'].get(),
            confirm
        )
    finally:
        ELEMENTS['button_import']['state'] = tkinter.NORMAL

def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble, 
        # and you should call it again with exc=NULL to revert the effect""" 
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")
 
 
def stop_thread(thread):
    _async_raise(thread.ident, SystemExit)

if __name__ == '__main__':

    simple_log.OUTPUT_METHODS.append(logout)

    window = tkinter.Tk()
    window.title('调度配置同步')
    window.grid(3, 8, 1, 1)
    
    thread = None

    
    def import_click():
        thread = threading.Thread(None, import_file, 'import_file')
        thread.start()

    ELEMENTS['label_file_name'] = tkinter.ttk.Label(window, text='配置文件(xlsx)')
    ELEMENTS['label_project_name'] = tkinter.ttk.Label(window, text='项目名称')
    ELEMENTS['label_tenant_name'] = tkinter.ttk.Label(window, text='租户名称')
    ELEMENTS['label_env_name'] = tkinter.ttk.Label(window, text='环境名称')
    ELEMENTS['button_choose_file'] = tkinter.ttk.Button(window, text='选择...', command=select_file)
    ELEMENTS['button_import'] = tkinter.ttk.Button(window, text='导入', command=import_click)
    ELEMENTS['input_file_name'] = tkinter.ttk.Entry(window, width=50)
    ELEMENTS['input_project_name'] = tkinter.ttk.Entry(window)
    ELEMENTS['input_tenant_name'] = tkinter.ttk.Entry(window)
    ELEMENTS['input_env_name'] = tkinter.ttk.Entry(window)
    
    s1 = tkinter.Scrollbar(window, orient=tkinter.VERTICAL)
    # s1.pack(side=tkinter.RIGHT, fill=tkinter.Y)
    s1.grid(row=4, column=3, pady=5, sticky=tkinter.NSEW)

    ELEMENTS['text_log'] = tkinter.Text(window, yscrollcommand=s1.set)
    s1.config(command=ELEMENTS['text_log'].yview)

    ELEMENTS['label_file_name'].grid(row=0, column=0, sticky=tkinter.E)
    ELEMENTS['label_project_name'].grid(row=1, column=0, sticky=tkinter.E)
    ELEMENTS['label_tenant_name'].grid(row=2, column=0, sticky=tkinter.E)
    ELEMENTS['label_env_name'].grid(row=3, column=0, sticky=tkinter.E)

    ELEMENTS['input_project_name'].insert(0, '[dw_main][1.1]')
    ELEMENTS['input_tenant_name'].insert(0, 'etl')
    ELEMENTS['input_env_name'].insert(0, 'etl')

    ELEMENTS['input_file_name'].grid(row=0, column=1, padx=5, pady=5, sticky=tkinter.EW)
    ELEMENTS['input_project_name'].grid(row=1, column=1, padx=5, pady=5, sticky=tkinter.EW)
    ELEMENTS['input_tenant_name'].grid(row=2, column=1, padx=5, pady=5, sticky=tkinter.EW)
    ELEMENTS['input_env_name'].grid(row=3, column=1, padx=5, pady=5, sticky=tkinter.EW)

    ELEMENTS['button_choose_file'].grid(row=0, column=2, columnspan=2, padx=5, pady=5, sticky=tkinter.EW)
    ELEMENTS['button_import'].grid(row=1, column=2, columnspan=2, padx=5, pady=5, sticky=tkinter.EW)

    ELEMENTS['text_log'].grid(row=4, column=0, columnspan=3, padx=(5, 0), pady=5)
    
    def on_closing():
        # 关闭窗口时提示
        if thread and thread.isAlive():
            if tkinter.messagebox.askokcancel('提示', '子任务还在执行，关闭程序会导致线程驻留后台，需要在‘任务管理器’中手动结束。\n\n确定要退出吗？'):
                try:
                    stop_thread(thread)
                except Exception as e:
                    quit(-1)
                finally:
                    window.destroy()
                
        else:
            window.destroy()
    window.protocol("WM_DELETE_WINDOW", on_closing)

    window.mainloop()