import PySide2


class Category:
    """
    树节点的数据
    """
    IDRole = PySide2.QtCore.Qt.ItemDataRole.UserRole + 1
    ParentIdRole = PySide2.QtCore.Qt.ItemDataRole.UserRole + 2

    def __init__(self, category_name=None, curr_id=None, parent_id=None):
        self.category_name = category_name
        self.id = curr_id
        self.parent_id = parent_id

    def set_category_name(self, category_name):
        self.category_name = category_name

    def set_id(self, id):
        self.id = id

    def set_parent_id(self, parent_id):
        self.parent_id = parent_id
