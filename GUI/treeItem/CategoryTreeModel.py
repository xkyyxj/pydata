import PySide2
from PySide2.QtCore import QAbstractItemModel, QModelIndex

from Data.DataCenter import DataCenter
from GUI.treeItem.Category import Category
from GUI.treeItem.TreeItem import TreeItem


class CategoryTreeModel(QAbstractItemModel):
    signals = []

    def __init__(self, parent=None):
        super().__init__(parent)
        self.root_item: TreeItem = TreeItem()
        # 做初始化逻辑
        data_center = DataCenter.get_instance()
        query_sql = "select * from ana_category"
        ret_val = data_center.common_query(query_sql)
        category_list = []
        for item in ret_val:
            temp_category = Category()
            temp_category.id = item[3]
            temp_category.parent_id = item[1]
            temp_category.category_name = item[2]
            category_list.append(temp_category)
        self.root_item.init_with_data(category_list)

    def flags(self, index):
        if not index.isValid():
            return 0
        return PySide2.QtCore.Qt.ItemFlag.ItemIsSelectable | PySide2.QtCore.Qt.ItemFlag.ItemIsEnabled

    def columnCount(self, parent=None, *args, **kwargs):
        return self.root_item.column_count()

    def data(self, index, role=None):
        if not index.isValid():
            return None
        if role != PySide2.QtCore.Qt.DisplayRole and role != PySide2.QtCore.Qt.EditRole and role != Category.IDRole:
            return None

        data: TreeItem = self.get_item(index)
        if role == Category.IDRole:
            return data.data(Category.IDRole)
        return data.data(PySide2.QtCore.Qt.DisplayRole)

    def get_item(self, index):
        if index.isValid():
            data = index.internalPointer()
            return data
        return self.root_item

    def headerData(self, section, orientation, role=None):
        if orientation == PySide2.QtCore.Qt.Horizontal and role == PySide2.QtCore.Qt.DisplayRole:
            return self.root_item.data(section)
        return None

    def index(self, row, column, parent=None, *args, **kwargs):
        if parent.isValid() and parent.column() != 0:
            return QModelIndex()
        parent_item: TreeItem = self.get_item(parent)

        child_item: TreeItem = parent_item.child(row)
        if child_item is not None:
            return super().createIndex(row, column, child_item)
        else:
            return QModelIndex()

    def parent(self, index) -> PySide2.QtCore.QModelIndex:
        if not index.isValid():
            return QModelIndex()

        child_item: TreeItem = self.get_item(index)
        parent_item: TreeItem = child_item.parent()

        if parent_item is self.root_item:
            return QModelIndex()

        return super().createIndex(parent_item.child_number(), 0, parent_item)

    def rowCount(self, parent=None, *args, **kwargs):
        parent_item = self.get_item(parent)
        return parent_item.child_count()

    def setData(self, index, value, role=None):
        if role != PySide2.QtCore.Qt.EditRole:
            return False

        item = self.get_item(index)
        result = item.setData(value, PySide2.QtCore.Qt.DisplayRole)
        if result:
            super().dataChanged(index, index, (role,))
        return result
