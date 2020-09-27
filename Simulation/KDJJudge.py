import pandas
import pandas_ta as ta


def kdj_judge(base_data):
    """
    通过KDJ指标来判定买入卖出时机，并且决定买入卖出的百分比
    :param base_data:
    :return:
    """
    default_length = 9
    default_signal = 3
    if not base_data.empty and len(base_data) > 0:
        ret_data = pandas.DataFrame(columns=('flag', 'percent'))
        high = base_data['high']
        low = base_data['low']
        close = base_data['close']
        kdj_ret = ta.kdj(high, low, close, length=default_length, signal=default_signal)
        if kdj_ret.empty or len(kdj_ret) < default_length:
            return None
        # 初始化返回结果的第一条数据
        for i in range(0, default_length):
            temp_dict = {
                "flag": 0,
                'percent': 0
            }
            ret_data = ret_data.append(temp_dict, ignore_index=True)
        ret_data = ret_data.append(temp_dict, ignore_index=True)
        for i in range(default_length, len(kdj_ret)):
            # 第一条判定准则：
            # 1. D值小于20，并且K值从下向上穿越过D值，买入
            # 2. D值大于80，并且K值从上到下穿越过D值，卖出
            is_pre_low = kdj_ret.at[i - 1, 'K_9_3'] < kdj_ret.at[i - 1, 'D_9_3']
            is_curr_high = kdj_ret.at[i, 'K_9_3'] >= kdj_ret.at[i, 'D_9_3']
            if kdj_ret.at[i, 'D_9_3'] < 20 and is_pre_low and is_curr_high:
                temp_dict = {
                    "flag": 1,
                    'percent': 0.1
                }
                ret_data = ret_data.append(temp_dict, ignore_index=True)
            elif kdj_ret.at[i, 'D_9_3'] > 80:
                is_pre_high = kdj_ret.at[i - 1, 'K_9_3'] > kdj_ret.at[i - 1, 'D_9_3']
                is_curr_low = kdj_ret.at[i, 'K_9_3'] <= kdj_ret.at[i, 'D_9_3']
                if is_pre_high and is_curr_low:
                    temp_dict = {
                        "flag": -1,
                        'percent': 0.1
                    }
                    ret_data = ret_data.append(temp_dict, ignore_index=True)
            else:
                temp_dict = {
                    "flag": 0,
                    'percent': 0
                }
                ret_data = ret_data.append(temp_dict, ignore_index=True)
        return ret_data

