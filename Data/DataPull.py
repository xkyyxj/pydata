#! /usr/bin/env python
"""
数据拉取，从Tushare开源平台获取
"""
import tushare


class DataPull:

    def __init__(self):
        # 20c67a94089468176b4f9185aad4051d523edebec1a6fea334d47038 -- 有权限
        # 09aee96979d53b07bca863a4854ef8f549ee5960cddabda75e1cf518 -- 无权限
        self.__pro = tushare.pro_api('20c67a94089468176b4f9185aad4051d523edebec1a6fea334d47038')

    def pull_data(self, code, start_date='20180101', end_date="20181231"):
        """
        获取股票的基本信息或者是指数的基本信息
        :param code: 股票编码
        :param end_date: 结束日期
        :type start_date: 开始日期
        :return 返回股票基本信息
        """
        data = self.__pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
        return data

    def pull_all_one_day(self, trade_date):
        data = self.__pro.daily(trade_date=trade_date)
        return data

    def fetch_stock_index_info(self, index_code='000001.SH', start_date='20180101', end_date='20181231'):
        """
        获取股票交易指数的日常信息
        :param end_date:
        :param start_date:
        :param index_code:
        :return:
        """
        data = self.__pro.index_daily(ts_code=index_code, start_date=start_date, end_date=end_date)
        return data

    def pull_daily_info(self, code):
        data = self.__pro.daily()

    def fetch_adj_factor_by_code(self, code):
        data = self.__pro.adj_factor(ts_code=code)
        return data

    def fetch_adj_factor_by_date(self, trade_date):
        data = self.__pro.adj_factor(trade_date=trade_date)
        return data

    def fetch_adj_daily_info(self, code, start_date='20180101', end_date='20181231'):
        """
        通过接口获取调整后的信息
        :param end_date:
        :param start_date:
        :param code: 股票ts_code
        :return: 获取到的数据
        """
        return tushare.pro_bar(pro_api=self.__pro, ts_code=code, adj='hfq', start_date=start_date, end_date=end_date)

    def fetch_stock_list(self):
        """
        获取所有的股票的基本信息，包括名称编码等内容，目前只获取上市公司的信息
        :return:
        """
        data = self.__pro.stock_basic(list_status='L')
        return data

    def fetch_index_list(self, market):
        data = self.__pro.index_basic(market=market)
        return data
