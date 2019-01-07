#!/usr/bin/env python3
"""
结果输出
"""
import os
import pandas


windows_directory = 'D:\\pystock'


def csv_output(stock_code, data, file_name, index=False, extra_content=None):
    """
    输出到CSV文件，利用DataFrame的to_csv比较方便简单，目前只支持这一种方式
    :param file_name:
    :param stock_code:
    :param data:
    :return:
    """
    create_workspace()
    if stock_code is not None:
        create_spe_dir(stock_code)
        path = os.path.join(windows_directory, [stock_code, 'file1'])
    else:
        path = os.path.join(windows_directory, 'file_' + file_name)
    if os.path.exists(path):
        os.remove(path)
    output_file = open(path, 'w')
    if isinstance(data, pandas.DataFrame):
        data.to_csv(output_file, index=index)
    if extra_content is not None and len(extra_content) > 0:
        output_file.write(extra_content)
    output_file.close()


def create_workspace():
    if os.name == 'nt':
        if not os.path.exists(windows_directory):
            os.mkdir(windows_directory)


def create_spe_dir(stock_code):
    create_workspace()
    if os.name == 'nt':
        path = os.path.join(windows_directory, stock_code)
        os.mkdir(path)


def create_write_content(data):
    ret_value = ''
    if isinstance(data, tuple):
        for item in data:
            if isinstance(item, tuple):
                for cell in item:
                    ret_value += str(cell) + ","
                ret_value += "\n"
    return ret_value
