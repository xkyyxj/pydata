import sys
from PySide2 import QtCore, QtWidgets, QtGui
from PySide2.QtCore import QDir
from PySide2.QtWidgets import QSizePolicy, QTreeView, QFileSystemModel

from GUI.treeItem.CategoryTreeModel import CategoryTreeModel
from ui_config.main_window import Ui_MainWindow


def tree_node_selected(selected, un_selected):
    print("hehedada ahahahhahahhahahah")


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

        tree_model = CategoryTreeModel()
        # self.tree_view = QtWidgets.QTreeView(self)
        # self.tree_view.setModel(tree_model)
        # self.tree_view.setFixedWidth(200)
        # self.tree_view.setFixedHeight(600)
        # self.tree_view.selectionModel().selectionChanged.connect(tree_node_selected)
        # sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        # sizePolicy.setHorizontalStretch(1)
        # sizePolicy.setVerticalStretch(0)
        # sizePolicy.setHeightForWidth(self.tree_view.sizePolicy().hasHeightForWidth())
        # self.tree_view.setSizePolicy(sizePolicy)
        # self.ui.treeView.setSelectionMode(QtWidgets.QAbstractItemView.)
        self.ui.treeView.setModel(tree_model)
        self.ui.treeView.selectionModel().selectionChanged.connect(tree_node_selected)


def init_gui():
    # 创建应用程序和对象
    app = QtWidgets.QApplication(sys.argv)
    ex = MainWindow()
    ex.show()
    sys.exit(app.exec_())
