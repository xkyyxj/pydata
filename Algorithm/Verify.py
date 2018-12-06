#!/usr/bin/env python3
"""
验证先关的交易策略是否生效
"""
import pandas
import matplotlib.pyplot as plt


def verify(base_data):
    """
    根据算出分数确定交易策略是否生效
    :param base_data:
    :return:
    """
    shift_af_close = base_data['af_close'].shift(-7)
    base_data['win_percent'] = (shift_af_close - base_data['af_close']) / base_data['af_close']
    base_data['win_price'] = shift_af_close - base_data['af_close']
    # predict_win = base_data[base_data['score'] > 0]
    predict_win = pandas.DataFrame(columns=base_data.columns)
    pre_low = True
    for i in range(len(base_data)):
        if base_data.at[i, 'ma5'] > base_data.at[i, 'ma10']:
            if pre_low and base_data.at[i, 'ma3'] >= 0:
                predict_win = predict_win.append(base_data.iloc[i])
            pre_low = False
        elif base_data.at[i, 'ma5'] < base_data.at[i, 'ma10']:
            pre_low = True

    temp_win2 = pandas.DataFrame(columns=base_data.columns)
    for i in range(len(base_data)):
        if base_data.at[i, 'pma2'] < -0.04:
            pre_low = True
        if base_data.at[i, 'af_close_percent'] > 0.04 and pre_low:
            temp_win2 = temp_win2.append(base_data.iloc[i])
            pre_low = False

    # temp_win2['win_percent'].plot()

    # print("win_count")
    # print(len(base_data[base_data['win_percent'] > 0.08]))
    temp_win = base_data[base_data['win_percent'] > 0.08]
    plt.figure(1, dpi=300)
    print('real can win %d' % (len(temp_win),))
    ax1 = plt.subplot(211)
    temp_win['win_percent'].plot()
    ax2 = plt.subplot(212)
    base_data['af_close'].plot()
    # down_info = base_data[base_data['win_percent'] < 0.08]
    # ma_60_slope_down_pct = len(down_info[down_info['ma60slope'] < 0]) / len(down_info)
    # ma_30_slope_down_pct = len(down_info[down_info['ma30slope'] < 0]) / len(down_info)
    # ma_10_slope_down_pct = len(down_info[down_info['ma10slope'] < 0]) / len(down_info)
    # print("down_60_percnet: %-10.3f, down_30_percent: %-10.3f， down_10_percent: %-10.3f" % (ma_60_slope_down_pct, ma_30_slope_down_pct, ma_10_slope_down_pct))
    # down_info = down_info.sort_values(by=['win_percent'])
    # print("calculate win count")
    # print(len(predict_win))
    #
    # temp_predict_win = base_data[base_data['score'] > 0]
    # temp_predict_win['win_percent'].plot()
    # plt.scatter(base_data['score'], base_data['win_percent'])
    # plt.scatter(predict_win['score'], predict_win['win_percent'])
    # predict_win['win_percent'].plot()


def real_verify(base_data):
    shift_af_close = base_data['af_close'].shift(-5)
    base_data['win_percent'] = (shift_af_close - base_data['af_close']) / base_data['af_close']
    base_data['win_price'] = shift_af_close - base_data['af_close']

    plt.figure(1, dpi=300)
    ax1 = plt.subplot(311)
    base_data['af_close'].plot()
    ax1 = plt.subplot(312)
    base_data['score'].plot()
    temp_win = base_data[base_data['score'] > 0]
    ax2 = plt.subplot(313)
    plt.scatter(base_data['score'], base_data['win_percent'], s=1)
    # plt.scatter(temp_win['score'], temp_win['win_percent'])

