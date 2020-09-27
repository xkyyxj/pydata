import os

import pandas_ta as ta
import Data.DataCenter as DataCenter
from matplotlib import pyplot as plt


class IndicatorAnalyzer:
    """
    指标分析器，使用pandas_ta做分析工作
    """

    def __init__(self, data_center: DataCenter):
        self.__indicator_name = None
        self.__data_center: DataCenter = data_center

    def set_indicator_name(self, indicator_name):
        """
        设置分析指标的名称
        :param indicator_name:
        :return:
        """
        self.__indicator_name = indicator_name

    def start_analyze(self, stock_code):
        data_frame = self.__data_center.fetch_base_data(stock_code)
        # ret_val = ta.stdev(data_frame['close'], length=10)
        ret_val = ta.adx(data_frame['high'], data_frame['low'], data_frame['close'])
        self.draw_close_and_rst(data_frame['close'], ret_val, stock_code)
        return ret_val

    @staticmethod
    def draw_close_and_rst(close, rst, stock_code):
        fig, ax = plt.subplots()
        plt.grid(linestyle='-.')
        price_min_val = close.min()
        price_max_val = close.max()
        price_delta = price_max_val - price_min_val

        rst_min_val = rst.min()
        rst_max_val = rst.max()
        rst_delta = rst_max_val - rst_min_val

        rst = rst * (price_delta / rst_delta)
        rst = rst + (price_max_val - price_min_val - rst_min_val)
        close_line = ax.plot(close, label='close')
        adx_line = ax.plot(rst['ADX_14'], label='adx')
        plus_line = ax.plot(rst['DMP_14'], label='dmp')
        minus_line = ax.plot(rst['DMN_14'], label='dmn')
        ax.legend(loc='upper right')
        # 展示
        # fig.show()
        fig.savefig("adx" + os.sep + stock_code + '.svg', dpi=600, format='svg')
        plt.close(fig)
