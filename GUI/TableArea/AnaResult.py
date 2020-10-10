class AnaResult:
    def __init__(self, table_meta):
        self.table_meta = table_meta
        self.data = []
        self.db_columns = []
        self.display_head = []
        self.db_table_name = []
        self.column_type = []

    def init_data_from_db(self):
        pass

    def get_data(self):
        pass

    def get_db_columns(self):
        pass

    def get_display_head(self):
        pass

    def get_is_valid(self):
        pass

    def process_data_type(self):
        date_time_index = []
        for i in range(len(self.column_type)):
            if self.column_type[i] == 'date':
                date_time_index.append(i)

        # 处理日期类型字段
        for item in date_time_index:
            for data_item in self.data:
                data_item[item] = data_item[item].strftime("%Y%m%d")
