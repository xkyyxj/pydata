import pandas
import pandas_ta as ta

from Simulation.simulate import Simulate


def macd_judge(base_data, fast=12, slow=26, signal=9):
    """
    通过macd指标确定买卖时机点
    DIFF线从下向上穿越DEA线时买入
    DIFF线从上向下穿越DEA线时卖出
    :param signal:
    :param slow:
    :param fast:
    :param base_data:
    :return:
    """
    if not base_data.empty and len(base_data) > 0:
        ret_data = pandas.DataFrame(columns=('flag', 'percent'))
        close = base_data['close']
        macd_ret = ta.macd(close, fast, slow, signal=signal)
        for i in range(1, len(macd_ret)):
            if macd_ret[i, 'macd'] is not None and macd_ret[i, 'signal'] is not None and \
                macd_ret[i, 'macd'] >= macd_ret[i, 'signal']:
                if macd_ret[i - 1, 'macd'] < macd_ret[i - 1, 'signal']:
                    # 即是所谓的diff线从下向上穿越了DEA线，考虑买入
                    temp_dict = {
                        "flag": Simulate.BUY_FLAG,
                        'percent': 0.1
                    }
                    ret_data = ret_data.append(temp_dict, ignore_index=True)
            elif macd_ret[i, 'macd'] is not None and macd_ret[i, 'signal'] is not None and \
                macd_ret[i, 'macd'] < macd_ret[i, 'signal']:
                if macd_ret[i - 1, 'macd'] > macd_ret[i - 1, 'signal']:
                    # 即是所谓的diff线从上向下穿越了DEA线，考虑卖出
                    temp_dict = {
                        "flag": Simulate.SOLD_FLAG,
                        'percent': 0.1
                    }
                    ret_data = ret_data.append(temp_dict, ignore_index=True)
            else:
                temp_dict = {
                    'flag': Simulate.DO_NOTHING,
                    'percent': 0
                }
                ret_data = ret_data.append(temp_dict, ignore_index=True)
        return ret_data
