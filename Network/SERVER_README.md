# Network 服务端模块

该模块提供了基于 `FastAPI` 框架的服务端网络请求处理功能，用于构建高性能的RESTful API服务。

## 功能特点

- 基于FastAPI框架，提供高性能的异步Web服务
- 自动生成交互式API文档（Swagger UI和ReDoc）
- 支持请求验证和序列化（基于Pydantic）
- 内置CORS支持
- 完善的日志记录和异常处理机制
- 支持应用生命周期事件处理

## 安装依赖

在使用此模块之前，请确保安装了所需依赖：

```bash
pip install fastapi uvicorn[standard] pydantic
```

或者使用requirements.txt文件：

```bash
pip install -r requirements.txt
```

## 使用示例

### 直接运行服务

```bash
# 在项目根目录下运行
python -m Network.server
```

或者在项目根目录下创建一个主入口文件：

```python
# main.py
from Network.server import run_server

if __name__ == "__main__":
    run_server(host="0.0.0.0", port=8000)
```

然后运行：
```bash
python main.py
```

### 作为模块集成到现有应用

```python
from Network.server import app
import uvicorn

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

## API接口

服务启动后，可以通过以下URL访问：

- 主页: http://localhost:8000/
- API文档: http://localhost:8000/docs
- ReDoc文档: http://localhost:8000/redoc
- 健康检查: http://localhost:8000/health

### 可用接口

1. `GET /` - 根路径，返回欢迎信息
2. `GET /health` - 健康检查接口
3. `GET /stock/{stock_code}` - 获取指定股票代码的数据
4. `POST /stock/data` - 根据请求参数获取股票数据
5. `PUT /stock/{stock_code}` - 更新指定股票代码的数据
6. `DELETE /stock/{stock_code}` - 删除指定股票代码的数据

## 访问API文档

FastAPI会自动生成交互式API文档：

1. Swagger UI: http://localhost:8000/docs
2. ReDoc: http://localhost:8000/redoc

通过这些文档界面，您可以查看所有可用的API端点、测试请求参数并查看响应格式。