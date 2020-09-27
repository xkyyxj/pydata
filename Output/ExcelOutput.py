class ExcelOutput:
    def __init__(self, file_path):
        """
        初始化excel输出内容
        """
        self.__file_path = file_path

    def begin_new_file(self, file_name):
        """
        开始写入一个新的文件
        TODO -- 完善该函数
        :param file_name:文件名称，包含全路径
        :return:
        """
        pass

    def output(self, data):
        """
        TODO -- 待完善
        输出数据到指定目录当中
        :param data: pandas.DataFrame，待输出数据
        :return:
        """
        pass
