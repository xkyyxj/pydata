import pandas

from Simulation.simulate import Simulate


def days_judge(base_data, up_days=3, window_days=4):
    """
    简单的上涨或者下降天数来判定，当上涨天数超过:param up_days的时候就买入
    当下降或者不动的天数到达了windows_days//2的时候，就卖出
    :param base_data:
    :param up_days:
    :param window_days:
    :return:
    """
    if base_data.empty or len(base_data) == 0:
        return None
    ret_data = pandas.DataFrame(columns=('flag', 'percent'))
    if len(base_data) < window_days:
        return None

    ret_data = pandas.DataFrame(columns=('flag', 'percent'))
    rolling_up_days = 0
    first_day_up = base_data.at[0, 'close'] > base_data.at[0, 'open']
    for i in range(0, window_days - 1):
        if base_data.at[i, 'close'] > base_data.at[i, 'open']:
            rolling_up_days = rolling_up_days + 1
        temp_dict = {
            "flag": Simulate.DO_NOTHING,
            'percent': 0
        }
        ret_data = ret_data.append(temp_dict, ignore_index=True)

    for i in range(window_days - 1, len(base_data)):
        if base_data.at[i, 'close'] > base_data.at[i, 'open']:
            rolling_up_days = rolling_up_days + 1

        if rolling_up_days >= up_days:
            temp_dict = {
                "flag": Simulate.BUY_FLAG,
                'percent': 0.1
            }
            ret_data = ret_data.append(temp_dict, ignore_index=True)
        elif (window_days - rolling_up_days) > (window_days // 2):
            temp_dict = {
                "flag": Simulate.SOLD_FLAG,
                'percent': 1
            }
            ret_data = ret_data.append(temp_dict, ignore_index=True)
        else:
            temp_dict = {
                "flag": Simulate.DO_NOTHING,
                'percent': 0.1
            }
            ret_data = ret_data.append(temp_dict, ignore_index=True)
        if first_day_up:
            rolling_up_days = rolling_up_days - 1
        first_day_up = base_data.at[i - window_days + 2, 'close'] > base_data.at[i - window_days + 2, 'open']
    return ret_data
