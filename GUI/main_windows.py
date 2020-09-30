import sys
from PySide2 import QtCore, QtWidgets, QtGui
from PySide2.QtCore import QDir
from PySide2.QtWidgets import QSizePolicy, QTreeView, QFileSystemModel

from Data.DataCenter import DataCenter
from GUI.TableArea.CommonAnaRst import CommonAnaRst
from GUI.TableArea.TableModel import MainTableModel
from GUI.treeItem.Category import Category
from GUI.treeItem.CategoryTreeModel import CategoryTreeModel
from ui_config.main_window import Ui_MainWindow


class MainWindow(QtWidgets.QMainWindow):

    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        # 树结构初始化

        # model = QFileSystemModel()
        # model.setRootPath(QDir.currentPath())
        #
        # tree = QTreeView(self)
        # tree.setFixedWidth(200)
        # tree.setFixedHeight(600)
        # tree.setModel(model)

        self.tree_model = CategoryTreeModel()
        self.ui.treeView.setModel(self.tree_model)
        self.ui.treeView.selectionModel().selectionChanged.connect(self.tree_node_selected)

        self.table_model = MainTableModel()
        self.ui.tableView.setModel(self.table_model)

    def ana_result_type(self, type, table_meta):
        rst = CommonAnaRst(table_meta)
        # rst.set_filter("where del_date is null")
        rst.init_data_from_db()
        self.table_model.set_ana_result(rst)

    def tree_node_selected(self, selected, un_selected):
        print("hehedada ahahahhahahhahahah")
        if selected.indexes() is None or len(selected.indexes()) == 0:
            return
        index = selected.indexes()[0]
        pk_field = index.data(Category.IDRole)

        # 查询一下是哪张表
        table_name = None
        pk_table_meta = None
        is_redis = False
        instance = DataCenter.get_instance()
        query_table_name = "select table_name, ana_category.pk_tablemeta, is_redis from ana_category join table_meta " \
                           "on ana_category.pk_tablemeta=table_meta.pk_tablemeta where pk_category='"
        query_table_name = query_table_name + str(pk_field) + "'"
        ret_val = instance.common_query(query_table_name)
        if len(ret_val) > 0:
            table_name = ret_val[0][0]
            pk_table_meta = ret_val[0][1]
            is_redis = ret_val[0][2] == 'Y'

        self.ana_result_type(table_name, pk_table_meta)


def init_gui():
    # 创建应用程序和对象
    app = QtWidgets.QApplication(sys.argv)
    ex = MainWindow()
    ex.show()
    sys.exit(app.exec_())
