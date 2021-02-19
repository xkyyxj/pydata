import pandas

import pandas_ta as ta

import Data


def initialize_ema():
    """
    初始化ema指标，然后存储到ema_value这张表里面
    :return:
    """
    data_center = Data.DataCenter.DataCenter.get_instance()
    all_stock_list = data_center.fetch_stock_list()
    for item in all_stock_list:
        initialize_ema_one(data_center, item[0])


def initialize_ema_one(data_center, ts_code):
    """
    单只股票的计算ema的值
    :param data_center:
    :param ts_code:
    :return:
    """
    # 查看一下表里面有没有数据，如果有的话，就不自动生成了
    exists = data_center.common_query("select 1 from ema_value where ts_code='" + ts_code + "'")
    if len(exists) > 0:
        return
    # 对每个指标做计算
    query_close_sql = "select ts_code, trade_date, close from stock_base_info where ts_code='"
    query_close_sql = query_close_sql + ts_code + "'"
    base_info_data = data_center.common_query_to_pandas(query_close_sql)
    close_series = base_info_data['close']
    if close_series is None or len(close_series) < 40:
        return
    # 对每个ema长度做计算
    write_data_frame = pandas.DataFrame(columns=())
    for i in range(3, 31):
        write_data_frame['ts_code'] = base_info_data['ts_code']
        write_data_frame['trade_date'] = base_info_data['trade_date']
        temp_ema = ta.ema(close_series, length=i)
        write_data_frame['ema_' + str(i)] = temp_ema
    temp_ema = ta.ema(close_series, length=35)
    write_data_frame['ema_35'] = temp_ema
    temp_ema = ta.ema(close_series, length=40)
    write_data_frame['ema_40'] = temp_ema
    data_center.common_write_data_frame(write_data_frame, 'ema_value')


def append_cal_ema():
    """
    增量计算ema的值
    :return:
    """
    data_center = Data.DataCenter.DataCenter.get_instance()
    stock_list = data_center.fetch_stock_list()
    for item in stock_list:
        ema_last_info_query = "select * from ema_value where ts_code='" + item[0] + "' order by trade_date desc limit 1"
        ema_last_day_info = data_center.common_query_to_pandas(ema_last_info_query)
        # 如果表里面没有值，那么就直接初始化计算一次好了
        if ema_last_day_info is None or len(ema_last_day_info) == 0:
            initialize_ema_one(data_center, item[0])
            continue

        # 增补计算逻辑
        # 首先拼接列信息
        column_list = ['ts_code', 'trade_date']
        for i in range(3, 31):
            column_list.append("ema_" + str(i))
        column_list.append("ema_35")
        column_list.append("ema_40")
        write_data_frame = pandas.DataFrame(columns=tuple(column_list))
        # 查询出所有的大于ema_last_day_date的股票基本信息
        query_sql = "select ts_code, trade_date, close from stock_base_info where ts_code='" + item[0] + "'"
        query_sql = query_sql + " and trade_date > '" + ema_last_day_info.at[0, 'trade_date'] + "' order by trade_date"
        base_infos = data_center.common_query_to_pandas(query_sql)
        if base_infos is None or base_infos.empty:
            continue

        for i in range(len(base_infos)):
            temp_dict = {
                "ts_code": item[0],
            }
            curr_close = base_infos.at[i, 'close']
            temp_dict['trade_date'] = base_infos.at[i, 'trade_date']
            for length in range(3, 31):
                ema_field_name = 'ema_' + str(length)
                # FIXME - 直接去write_data_frame的最后一条会不会取错了？应该没啥问题
                last_ema_val = write_data_frame.at[len(write_data_frame) - 1, ema_field_name] \
                    if len(write_data_frame) > 0 else ema_last_day_info.at[0, ema_field_name]
                temp_dict["ema_" + str(length)] = cal_ema_single_day(last_ema_val, curr_close, length)
            last_ema_val = write_data_frame.at[len(write_data_frame) - 1, ema_field_name] \
                if len(write_data_frame) > 0 else ema_last_day_info.at[0, ema_field_name]
            temp_dict["ema_35"] = cal_ema_single_day(last_ema_val, curr_close, 35)
            temp_dict["ema_40"] = cal_ema_single_day(last_ema_val, curr_close, 40)
            write_data_frame = write_data_frame.append(temp_dict, ignore_index=True)
        data_center.common_write_data_frame(write_data_frame, 'ema_value')


def cal_ema_single_day(last_day_ema, last_day_close, ema_length):
    ret_val = last_day_ema * (ema_length - 1) + last_day_close * 2
    ret_val = ret_val / (ema_length + 1)
    return ret_val

