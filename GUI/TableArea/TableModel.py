import PySide2
from PySide2.QtCore import QAbstractTableModel

from Data.DataCenter import DataCenter


class MainTableModel(QAbstractTableModel):
    PRIMARY_KEY_ROLE = PySide2.QtCore.Qt.UserRole + 1

    def __init__(self, parent=None):
        super().__init__(parent)
        self.table_data = []
        self.select_columns = None
        self.table_name = None
        self.filter = None
        self.display_head = []
        self.primary_key = None

    def rowCount(self, parent=None, *args, **kwargs):
        return len(self.table_data)

    def columnCount(self, parent=None, *args, **kwargs):
        return len(self.select_columns)

    def headerData(self, section, orientation, role=None):
        """

        :param section:
        :param orientation:
        :param role:
        :return:
        """
        if role == PySide2.QtCore.Qt.DisplayRole and orientation == PySide2.QtCore.Qt.Horizontal:
            return self.display_head[section] if section < len(self.display_head) else None
        return super().headerData(section, orientation, role)

    def data(self, index, role=None):
        if not index.isValid():
            return None

        if role == PySide2.QtCore.Qt.TextAlignmentRole:
            return int(PySide2.QtCore.Qt.AlignRight | PySide2.QtCore.Qt.AlignVCenter)
        elif role == PySide2.QtCore.Qt.DisplayRole:
            return self.table_data[index.row()][index.column()]
        elif role == self.PRIMARY_KEY_ROLE:
            return self.get_primary_key_value(index)
        return None

    def select_data(self):
        """
        查询数据，更新表格
        :return:
        """
        query_sql = ""
        for i in range(len(self.select_columns)):
            query_sql = query_sql + self.select_columns[i] + ","

        query_sql = query_sql + self.select_columns[len(self.select_columns) - 1]
        query_sql = query_sql + " from " + self.table_name + " "
        query_sql = query_sql + self.filter

        data_center = DataCenter.get_instance()
        query_rst = data_center.common_query(query_sql)
        for item in query_rst:
            self.table_data.append(item)
        super().endResetModel()

    def set_primary_key(self, primary_key):
        self.primary_key = primary_key

    def get_primary_key_value(self, index):
        row = index.row()
        column = 0
        for i in range(len(self.select_columns)):
            if self.select_columns[i] == self.primary_key:
                column = i
                break
        return self.table_data[row][column]

    def set_select_columns(self, columns):
        """
        设置表格数据有哪些列
        :param columns:
        :return:
        """
        self.select_columns = columns

    def set_table_name(self, table_name):
        """
        设置数据库表名
        :param table_name:
        :return:
        """
        self.table_name = table_name

    def set_filter(self, filter):
        """
        设置查询的过滤条件
        :param filter:
        :return:
        """
        self.filter = filter

    def set_display_head_info(self, display_head):
        """
        设置展示头部信息
        :param display_head:
        :return:
        """
        self.display_head = display_head

    def set_table_data(self, table_data):
        self.table_data = table_data

    def update_view(self):
        super().endResetModel()
