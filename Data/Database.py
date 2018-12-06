#! /usr/bin/env python
"""
数据存储，写入到数据库当中
"""
import datetime
import pymysql
import numpy as np
import pandas
from sqlalchemy import create_engine


class MySQLDB:
    def __init__(self):
        self.__con = pymysql.connect("localhost", "root", "123", "stock")
        self.__engine = create_engine('mysql+mysqlconnector://root:123@localhost:3306/stock')
        self.__cursor = self.__con.cursor()

    def write_stock_info(self, info):
        """
        调用pandas.DataFrame的to_sql方法， 将相关数据写入到数据库当中
        :param info:
        :return:
        """
        info.to_sql('stock_base_info', self.__engine, if_exists='append', index=False)

    def fetch_daily_info(self, code, start_date="20180101", end_date="20181231"):
        """
        从数据库当中获取每日交易的数据
        :return:
        """
        query_sql = "select * from stock_base_info where ts_code=%s and trade_date >= %s and trade_date <= %s"
        results = pandas.read_sql(query_sql, con=self.__engine, params=(code, start_date, end_date))

        # 下面的方法存在问题：对于数字类型来说，并不能够正常转换
        # self.__cursor.execute(query_sql, (code, start_date, end_date))
        # results = np.array(self.__cursor.fetchall())
        return results

    def fetch_adj_daily_info(self, code, start_date='20180101', end_date='20181231'):
        """
        tushare有接口，能够获取前后复权之后的股价信息，但是这个方法还未经验证，可以自己算前后复权价格
        但是像是对于前复权来说，似乎存储在数据库当中是没有意义的
        TODO -- 已完成，未测试
        :param code:
        :param start_date:
        :param end_date:
        :return:
        """
        query_sql = "select * from stock_adj_info where ts_code=%s and trade_date >= %s and trade_date <= %s"
        self.__cursor.execute(query_sql, (code, start_date, end_date))
        result = np.array(self.__cursor.fetchall())
        return result

    def fetch_stock_list(self, code):
        if code is not None and len(code) > 0 and not code.isspace():
            query_sql = "select * from stock_list where symbol=%s"
            self.__cursor.execute(query_sql, (code,))
            return self.__cursor.fetchall()
        else:
            query_sql = "select * from stock_list"
            self.__cursor.execute(query_sql)
            return self.__cursor.fetchall()

    def write_stock_list(self, data):
        """
        鉴于tushare获取股票的基本信息不能指定编码，因此data当中必然包含了所有的股票信息
        因此将此表删除然后重新插入
        :param data:
        :return:
        """
        delete_sql = "delete from stock_list"
        self.__cursor.execute(delete_sql)
        self.__con.commit()
        data.to_sql('stock_list', self.__engine, if_exists='append', index=False)

    def fetch_adj_factor(self, code):
        """
        从数据库当中获取复权因子
        :param code:
        :return: 复权因子信息
        """
        query_sql = "select * from adj_factor where ts_code=%s"
        result = pandas.read_sql(query_sql, con=self.__engine, params=(code,))
        return result

    def write_adj_factor(self, data):
        data.to_sql('adj_factor', self.__engine, if_exists='append', index=False)

