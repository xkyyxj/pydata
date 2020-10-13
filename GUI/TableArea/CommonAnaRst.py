from Data.DataCenter import DataCenter
from GUI.TableArea.AnaResult import AnaResult


class CommonAnaRst(AnaResult):
    def __init__(self, table_meta):
        super().__init__(table_meta)
        self.filter = None

    def set_filter(self, _filter):
        self.filter = _filter

    def init_data_from_db(self):
        """
        通用的从数据库查询数据的结果处理类
        :return:
        """
        data_center = DataCenter.get_instance()
        query_sql = "select table_name, is_redis from table_meta where pk_tablemeta='"
        query_sql = query_sql + str(self.table_meta) + "'"
        table_meta = data_center.common_query(query_sql)
        if table_meta is None or len(table_meta) == 0:
            return

        self.db_table_name = table_meta[0][0]
        is_redis = table_meta[0][1] == 'Y'
        if is_redis:
            return

        query_sql = "select column_name, display_name, columntype from table_column where pk_tablemeta='"
        query_sql = query_sql + str(self.table_meta) + "' order by display_order"
        table_infos = data_center.common_query(query_sql)
        for item in table_infos:
            self.db_columns.append(item[0])
            self.display_head.append(item[1])
            self.column_type.append(item[2])
        if len(self.db_columns) == 0:
            return

        query_sql = "select "
        for item in self.db_columns:
            query_sql = query_sql + item + ","
        query_sql = query_sql[:-1]
        query_sql = query_sql + " from " + self.db_table_name
        if self.filter is not None:
            query_sql = query_sql + " " + self.filter
        data = data_center.common_query(query_sql)
        for item in data:
            self.data.append(list(item))
        # 处理一下数据类型问题
        self.process_data_type()

