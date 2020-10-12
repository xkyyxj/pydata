import sys

import PySide2
from PySide2 import QtCore, QtWidgets, QtGui
from PySide2.QtCore import QDir
from PySide2.QtWidgets import QSizePolicy, QTreeView, QFileSystemModel, QAction

from Data.DataCenter import DataCenter
from GUI.CustUIItem.StockChartModel import StockChartModel
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

        # 设置相关的监听事件
        self.tree_model = CategoryTreeModel()
        self.ui.treeView.setModel(self.tree_model)
        self.ui.treeView.selectionModel().selectionChanged.connect(self.tree_node_selected)
        self.ui.tableView.doubleClicked.connect(self.table_row_double_clicked)

        self.table_model = MainTableModel()
        self.ui.tableView.setModel(self.table_model)
        self.ui.tableView.setSortingEnabled(True)

        # 设置表格的邮件菜单
        self.ui.tableView.setContextMenuPolicy(PySide2.QtCore.Qt.ActionsContextMenu)

        # 具体菜单项
        manual_flag = QAction(self.ui.tableView)
        manual_flag.setText("标记为手动")
        manual_flag.triggered.connect(self.mark_manual)  # 点击菜单中的“发送控制代码”执行的函数

        # tableView 添加具体的右键菜单
        self.ui.tableView.addAction(manual_flag)

        # K线图组件初始化
        data_center = DataCenter()
        self.view_model = StockChartModel(data_center)
        self.ui.openGLWidget.set_model(self.view_model)

    def ana_result_type(self, type, table_meta):
        rst = CommonAnaRst(table_meta)
        # rst.set_filter("where del_date is null")
        rst.init_data_from_db()
        self.table_model.set_ana_result(rst)

    def tree_node_selected(self, selected, un_selected):
        """
        树节点选中事件
        :param selected:
        :param un_selected:
        :return:
        """
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
        
    def table_row_double_clicked(self, index: PySide2.QtCore.QModelIndex):
        ts_code = index.data(MainTableModel.PRIMARY_KEY_ROLE)
        print(ts_code)
        self.view_model.set_curr_selected_stock(ts_code)
        pass

    def mark_manual(self):
        """
        将某行标记为选中
        :return:
        """
        selected_row = self.ui.tableView.currentIndex().row()
        if selected_row < 0:
            return

        # FIXME -- 此处写死了是这张表格
        if self.table_model.table_name != 'period_verify':
            return

        model_index = self.table_model.index(selected_row)
        self.table_model.data(model_index, MainTableModel.PRIMARY_KEY_ROLE)
        pass


def init_gui():
    # 创建应用程序和对象
    app = QtWidgets.QApplication(sys.argv)
    ex = MainWindow()
    ex.show()
    sys.exit(app.exec_())
