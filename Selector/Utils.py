import pandas

from Data.DataCenter import DataCenter


def merge_table_data(data_center: DataCenter, tables):
    """
    从多个表格里面查询出来股票编码，然后做个交集
    :param data_center:
    :param tables:
    :return:
    """
    if tables is None or len(tables) == 0:
        return pandas.Series()

    ret_series = pandas.Series()
    for item in tables:
        sql = "select ts_code from " + tables
        temp_data = data_center.common_query_to_pandas(sql)
        temp_series = temp_data['ts_code']
        ret_series = ret_series.append(temp_series, ignore_index=True)
    return ret_series.unique()
