class AnaResult:
    def __init__(self, table_meta):
        self.table_meta = table_meta
        self.data = []
        self.db_columns = []
        self.display_head = []
        self.db_table_name = []

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
