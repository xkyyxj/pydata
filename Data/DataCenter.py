#! /usr/bin/env python
"""
数据中心，负责获取数据以及将数据同步到本地MySQL数据当中
数据获取是通过Tushare开源平台获取的
"""
import datetime
import pandas
import Data.Database
import Data.DataPull


class DataCenter:

    def __init__(self):
        self.__database = Data.Database.MySQLDB()
        self.__datapull = Data.DataPull.DataPull()

    def fetch_base_data(self, stock_code, begin_date="20180101", end_date="20181231"):
        """
        获取股票的日交易信息，
        :param stock_code:
        :param begin_date:
        :param end_date:
        :return:
        """
        local_data = self.__database.fetch_daily_info(stock_code, begin_date, end_date)

        if local_data.size == 0:
            ret_value = self.__datapull.pull_data(stock_code, begin_date, end_date)
            self.__database.write_stock_info(ret_value)
        else:
            ret_value = local_data
            # ret_value = pandas.DataFrame(local_data, columns=('trade_date', 'ts_code', 'open', 'close', 'high', 'low',
            #                                                   'vol', 'amount', 'pre_close', 'change', 'pct_chg'))
            ret_value.sort_index(axis=1)
            # 获取没有数据的天数，此处需要往后推一天
            temp_date = ret_value.at[len(ret_value) - 1, 'trade_date']
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date += datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")
            if temp_date < end_date:
                after_data = self.__datapull.pull_data(stock_code, need_date, end_date)
                self.__database.write_stock_info(after_data)
                ret_value = ret_value.merge(after_data, how="outer")

            # 获取没有数据的天数，此处需要往前推一天
            temp_date = ret_value.at[0, 'trade_date']
            need_date = datetime.date(int(temp_date[0:4]), int(temp_date[4:6]), int(temp_date[6:8]))
            need_date -= datetime.timedelta(days=1)
            need_date = need_date.strftime("%Y%m%d")
            if temp_date > begin_date:
                before_data = self.__datapull.pull_data(stock_code, begin_date, need_date)
                self.__database.write_stock_info(before_data)
                ret_value = before_data.merge(ret_value, how="outer")
        return ret_value

    def fetch_adj_factor(self, ts_code, begin_date='20180101', end_date='20181231'):
        """
        从数据库当中获取复权因子，如果是复权因子没有包含最新的，那么重新从tushare接口获取
        :param end_date: 结束日期
        :param begin_date: 开始日期
        :param ts_code: 股票tushare编码
        :return:
        """
        date = datetime.date.today()
        date = date.strftime("%Y%m%d")
        local_data = self.__database.fetch_adj_factor(ts_code)
        local_data.sort_index(axis=1)
        data = self.__datapull.fetch_weight_factor(ts_code, date)
        if local_data.size <= 0 or (data.size > 0 and data.at[0, 'adj_factor'] !=
                                    local_data.at[len(local_data) - 1, 'adj_factor']):
            data = self.__datapull.fetch_weight_factor(ts_code)
            data.sort_index(axis=1)
            insert_data = data[data.trade_date > local_data.at[len(local_data) - 1, 'trade_date']] \
                if local_data.size > 0 else data
            self.__database.write_adj_factor(insert_data)
            ret_data = data[(data.trade_date >= begin_date) & (data.trade_date <= end_date)]
            ret_data.index = range(len(ret_data))   # 重新设置一下index，避免两Series相乘找不到对应位置
            return ret_data
        else:
            ret_data = local_data[(local_data.trade_date >= begin_date) & (local_data.trade_date <= end_date)]
            ret_data.index = range(len(ret_data))  # 重新设置一下index，避免两Series相乘找不到对应位置
            return ret_data

    def fetch_stock_list(self, code=None):
        local_data = self.__database.fetch_stock_list(code)
        if not local_data or (code is not None and len(code) > 0 and not code.isspace() and code not in local_data):
            stock_list = self.__datapull.fetch_stock_list()
            self.__database.write_stock_list(stock_list)
            return stock_list
        else:
            return local_data

    @staticmethod
    def process_data(base_data, adj_factor):
        """
        处理一下权重信息
        TODO -- 此处先仅处理后复权，后面再修正
        :param base_data:
        :param adj_factor:
        :return:
        """
        base_data['af_open'] = base_data['open'] * adj_factor['adj_factor']
        return base_data



