import PySide2
from PySide2.QtCore import QAbstractItemModel

from GUI.treeItem.Category import Category


class TreeItem:
    def __init__(self, category_data=None, parent_item=None):
        self.item_data = category_data
        self.parent_item = parent_item
        self.child_items = []

    def init_with_data(self, data: []):
        """
        根据传入数组构建一棵树
        :param data:
        :return:
        """
        for item in data[::-1]:
            if item.parent_id == self.data(Category.IDRole):
                self.child_items.append(TreeItem(item, self))
                data.remove(item)

        for child_item in self.child_items:
            child_item.init_with_data(data)

    def child(self, number):
        """
        获取第number个元素
        :param number:
        :return:
        """
        return self.child_items[number]

    def child_count(self):
        return len(self.child_items)

    def child_number(self):
        if self.parent_item is not None:
            self.parent_item.child_items.index(self)
        return 0

    def data(self, role):
        if self.item_data is None:
            return None
        value_map = {
            PySide2.QtCore.Qt.ItemDataRole.DisplayRole: self.item_data.category_name,
            PySide2.QtCore.Qt.ItemDataRole.EditRole: self.item_data.category_name,
            Category.IDRole: self.item_data.id,
            Category.ParentIdRole: self.item_data.parent_id,
        }
        return value_map[role]

    def insert_children(self, position: int,  data: Category):
        """
        插入子级数据
        :param position:
        :param data:
        :return:
        """
        if position < 0 or position > len(self.child_items):
            return False

        item = TreeItem(data, self)
        self.child_items.insert(position, item)
        return True

    def parent(self):
        """
        返回父级树节点
        :return:
        """
        return self.parent_item

    @staticmethod
    def column_count():
        return 1
