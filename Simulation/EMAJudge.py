import pandas
import pandas_ta as ta

from Simulation.simulate import Simulate


def ema_judge(base_data, ema_length=5, buy_up_count=3, win_pct=0.07, lost_pct=0.03):
    """
    通过EMA指标来短期判定买入卖出，机械性的
    :param buy_up_count: 当EMA连续上涨多少天之后就买入
    :param lost_pct: 损失百分比，当损失达到这种程度的时候就卖出
    :param win_pct: 盈利百分比，当盈利达到这个程度的时候就卖出
    :param ema_length: EMA的长度
    :param base_data: 基础数据
    :return:
    """
    if base_data.empty or len(base_data) == 0:
        return None
    ret_data = pandas.DataFrame(columns=('flag', 'percent'))
    close = base_data['close']
    ema_value = ta.ema(close, length=5)
    if len(ema_value) <= ema_length:
        return None

    temp_dict = {
        "flag": Simulate.DO_NOTHING,
        'percent': 0
    }
    ret_data = ret_data.append(temp_dict, ignore_index=True)

    up_count = 0
    already_buy = False
    buy_price = 0
    for i in range(1, len(ema_value)):
        temp_dict = {
            "flag": Simulate.DO_NOTHING,
            'percent': 0
        }
        if ema_value.iat[i] > ema_value.iat[i - 1]:
            if up_count > buy_up_count and not already_buy:
                temp_dict['flag'] = Simulate.BUY_FLAG
                temp_dict['percent'] = 0.5
                buy_price = base_data.at[i, 'close']
                already_buy = True
                ret_data = ret_data.append(temp_dict, ignore_index=True)
                continue
            up_count = up_count + 1
        else:
            up_count = 0

        # 分析下盈利情况，然后决定是否卖出或者继续持有
        if not already_buy:
            ret_data = ret_data.append(temp_dict, ignore_index=True)
            continue

        curr_price = base_data.at[i, 'close']
        curr_win_pct = (curr_price - buy_price) / buy_price
        if curr_win_pct > win_pct:
            temp_dict['flag'] = Simulate.SOLD_FLAG
            temp_dict['percent'] = 1
            already_buy = False
        elif curr_win_pct <= -lost_pct:
            temp_dict['flag'] = Simulate.SOLD_FLAG
            temp_dict['percent'] = 1
            already_buy = False
        ret_data = ret_data.append(temp_dict, ignore_index=True)
    return ret_data


