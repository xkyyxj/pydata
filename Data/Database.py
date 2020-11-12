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
        self.__con.autocommit(True)
        self.__engine = create_engine('mysql+mysqlconnector://root:123@localhost:3306/stock')
        self.__cursor = None

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

    def fetch_index_daily_info(self, code, start_date="20180101", end_date="20181231"):
        """
        从数据库当中获取指数的每日交易的数据
        :return:
        """
        query_sql = "select * from stock_index_baseinfo where ts_code=%s and trade_date >= %s and trade_date <= %s"
        results = pandas.read_sql(query_sql, con=self.__engine, params=(code, start_date, end_date))

        # 下面的方法存在问题：对于数字类型来说，并不能够正常转换
        # self.__cursor.execute(query_sql, (code, start_date, end_date))
        # results = np.array(self.__cursor.fetchall())
        return results

    def fetch_all_daily_info_by_date(self, start_date):
        """
        从数据库当中获取所有的股票的日交易信息，从@param start_date开始
        :param start_date: 获取的所有股票日交易信息的开始日期
        :return:
        """
        query_sql = "select * from stock_base_info where trade_date >= %s"
        results = pandas.read_sql(query_sql, con=self.__engine, params=(start_date,))
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
        self.__cursor = self.__con.cursor()
        query_sql = "select * from stock_adj_info where ts_code=%s and trade_date >= %s and trade_date <= %s"
        self.__cursor.execute(query_sql, (code, start_date, end_date))
        result = np.array(self.__cursor.fetchall())
        self.__con.commit()
        self.__cursor.close()
        return result

    def fetch_stock_list(self, code, market=['主板', '中小板'], where=''):
        self.__cursor = self.__con.cursor()
        has_where = False
        query_sql = "select * from stock_list"
        condition = []
        if code is not None and len(code) > 0 and not code.isspace():
            has_where = True
            condition.append(code)
            query_sql += " where symbol=%s"

        if market is not None and len(market) > 0:
            condition.extend(market)
            market_where = "market in ("
            for mark in market:
                market_where += '%s,'
            market_where += "'')"
            query_sql += " and " + market_where if has_where else " where " + market_where
            has_where = True

        if where is not None and len(where) > 0 and not where.isspace():
            query_sql += " and " + where if has_where else " where " + where
        query_sql += " order by ts_code"
        self.__cursor.execute(query_sql, condition)
        data = self.__cursor.fetchall()
        self.__con.commit()
        self.__cursor.close()
        return data

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
        query_sql = "select adj_factor.* from stock_base_info left join adj_factor " \
                    "on stock_base_info.trade_date=adj_factor.trade_date where adj_factor.ts_code=%s"
        result = pandas.read_sql(query_sql, con=self.__engine, params=(code,))
        return result

    def fetch_all_adj_factor_by_date(self, start_date, end_date='99991231'):
        """
        一次性从数据库当中获取所有股票的复权因子，从@param start_date之后开始
        :param start_date:
        :return:
        """
        query_sql = "select * from adj_factor where trade_date>=%s and trade_date in " \
                    "(select trade_date from stock_base_info where ts_code=adj_factor.ts_code and " \
                    "trade_date>=%s)"
        result = pandas.read_sql(query_sql, con=self.__engine, params=(start_date, start_date))
        return result

    def fetch_adj_factor_by_code_date(self, stock_code, begin_date, end_date):
        """
        从数据库当中获取复权因子，根据股票编码以及开始结束日期过滤
        :param end_date:
        :param begin_date:
        :param stock_code:
        :return:
        """
        query_sql = "select adj_factor.* from stock_base_info left join adj_factor " \
                    "on stock_base_info.trade_date=adj_factor.trade_date " \
                    "and stock_base_info.ts_code=adj_factor.ts_code where adj_factor.ts_code=%s " \
                    "and adj_factor.trade_date>=%s and adj_factor.trade_date<= %s"
        result = pandas.read_sql(query_sql, con=self.__engine, params=(stock_code, begin_date, end_date))
        return result

    def write_adj_factor(self, data):
        data.to_sql('adj_factor', self.__engine, if_exists='append', index=False)

    def is_exist_base_data(self, stock_code, date):
        self.__cursor = self.__con.cursor()
        query_sql = "select trade_date from stock_base_info where ts_code=%s and trade_date like %s " \
                    "order by trade_date limit 1"
        self.__cursor.execute(query_sql, (stock_code, date + "%"))
        result = self.__cursor.fetchall()
        self.__con.commit()
        self.__cursor.close()
        return len(result) > 0 and result[0][0]

    def is_exist_index_base_data(self, stock_code, date):
        self.__cursor = self.__con.cursor()
        query_sql = "select trade_date from stock_index_baseinfo where ts_code=%s and trade_date like %s " \
                    "order by trade_date limit 1"
        self.__cursor.execute(query_sql, (stock_code, date + "%"))
        result = self.__cursor.fetchall()
        self.__con.commit()
        self.__cursor.close()
        return len(result) > 0 and result[0][0]

    def is_exist_adj_factor(self, stock_code, date):
        self.__cursor = self.__con.cursor()
        query_sql = "select trade_date from adj_factor where ts_code=%s and trade_date like %s " \
                    "order by trade_date limit 1"
        self.__cursor.execute(query_sql, (stock_code, date + "%"))
        result = self.__cursor.fetchall()
        self.__con.commit()
        self.__cursor.close()
        return len(result) > 0 and result[0][0]

    def write_index_list(self, index_list):
        """
        将指数的基本信息写入到数据库当中
        :param index_list:
        :return:
        """
        if len(index_list) > 0:
            delete_sql = "delete from stock_index where market like %s"
            self.__cursor.execute(delete_sql, (index_list.at[0, 'market'],))
            self.__con.commit()
            index_list.to_sql('stock_index', self.__engine, if_exists='append', index=False)

    def write_index_daily_info(self, daily_info):
        """
        将指数的日线信息写入到数据库当中
        :param daily_info:
        :return:
        """
        daily_info.to_sql('stock_index_baseinfo', self.__engine, if_exists='append', index=False)

    def delete_stock_list(self):
        """
        清空stock_list
        :return:
        """
        self.__cursor = self.__con.cursor()
        sql = "delete from stock_list"
        self.__cursor.execute(sql)

    def common_query(self, sql):
        """
        通用查询，返回结果，只是元组的集合
        :param sql:
        :return:
        """
        self.__cursor = self.__con.cursor()
        self.__cursor.execute(sql)
        result = self.__cursor.fetchall()
        self.__cursor.close()
        return result

    def common_query_to_pandas(self, sql) -> pandas.DataFrame:
        """
        通用查询，返回pandas.DataFrame
        :param sql:
        :return:
        """
        result = pandas.read_sql(sql, con=self.__engine)
        return result

    def common_write_data_frame(self, data_frame, table_name):
        """
        通用的将pandas.DataFrame写入到数据库当中
        :param table_name: 数据库表名
        :param data_frame: 将要写入的数据，类型是pandas.DataFrame
        :return:
        """
        data_frame.to_sql(table_name, self.__engine, if_exists='append', index=False)
