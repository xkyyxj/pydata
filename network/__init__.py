"""
network 网络模块
提供HTTP客户端和服务端功能
"""

# 从http_client模块导入（如果之前创建了的话）
# try:
#     from .http_client import HttpClient, create_http_client, quick_get, async_quick_get
#     __all__ = [
#         "HttpClient",
#         "create_http_client",
#         "quick_get",
#         "async_quick_get"
#     ]
# except ImportError:
#     # 如果没有http_client模块，则不导入
#     pass
#
# # 导入服务端应用
# try:
#     from .server import app, run_server
#     __all__.extend(["app", "run_server"])
# except ImportError:
#     pass

__all__ = [
    "app",
    "run_server"
]

from network.server import app, run_server
