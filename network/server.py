"""
服务端网络请求处理模块
使用FastAPI框架实现RESTful API服务
FastAPI是生产环境中广泛使用的现代、快速（高性能）的Web框架
"""

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uvicorn
import json
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建FastAPI应用实例
app = FastAPI(
    title="Stock data Service API",
    description="提供stock数据相关的RESTful API服务",
    version="1.0.0"
)

# 添加CORS中间件，允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该指定具体的域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 数据模型定义
class StockDataRequest(BaseModel):
    """stock数据请求模型"""
    stock_code: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    fields: Optional[List[str]] = None

class StockDataResponse(BaseModel):
    """stock数据响应模型"""
    stock_code: str
    data: List[Dict[str, Any]]
    count: int

class HealthCheckResponse(BaseModel):
    """健康检查响应模型"""
    status: str
    timestamp: str
    service: str

# 路由定义
@app.get("/", tags=["Health"])
async def root():
    """根路径，返回欢迎信息"""
    return {"message": "Welcome to Stock data Service API", "version": "1.0.0"}

@app.get("/health", response_model=HealthCheckResponse, tags=["Health"])
async def health_check():
    """健康检查接口"""
    from datetime import datetime
    return HealthCheckResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        service="Stock data Service"
    )

@app.get("/stock/{stock_code}", response_model=StockDataResponse, tags=["Stock data"])
async def get_stock_data(stock_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    获取指定stock代码的数据
    
    Args:
        stock_code: stock代码
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        
    Returns:
        stock数据
    """
    logger.info(f"Received request for stock data: {stock_code}")
    
    # 模拟数据获取逻辑
    mock_data = [
        {"date": "2023-01-01", "open": 100.0, "close": 105.0, "high": 107.0, "low": 99.0, "volume": 1000000},
        {"date": "2023-01-02", "open": 105.0, "close": 102.0, "high": 106.0, "low": 101.0, "volume": 800000},
    ]
    
    return StockDataResponse(
        stock_code=stock_code,
        data=mock_data,
        count=len(mock_data)
    )

@app.post("/stock/data", response_model=StockDataResponse, tags=["Stock data"])
async def post_stock_data(request: StockDataRequest):
    """
    根据请求参数获取stock数据
    
    Args:
        request: stock数据请求对象
        
    Returns:
        stock数据
    """
    logger.info(f"Received POST request for stock data: {request.stock_code}")
    
    # 模拟数据获取逻辑
    mock_data = [
        {"date": "2023-01-01", "open": 100.0, "close": 105.0, "high": 107.0, "low": 99.0, "volume": 1000000},
        {"date": "2023-01-02", "open": 105.0, "close": 102.0, "high": 106.0, "low": 101.0, "volume": 800000},
    ]
    
    return StockDataResponse(
        stock_code=request.stock_code,
        data=mock_data,
        count=len(mock_data)
    )

@app.put("/stock/{stock_code}", tags=["Stock data Management"])
async def update_stock_data(stock_code: str, data: dict):
    """
    更新指定stock代码的数据
    
    Args:
        stock_code: stock代码
        data: 更新的数据
        
    Returns:
        更新结果
    """
    logger.info(f"Received PUT request to update stock data: {stock_code}")
    
    # 模拟更新逻辑
    return {"message": f"Stock data for {stock_code} updated successfully", "data": data}

@app.delete("/stock/{stock_code}", tags=["Stock data Management"])
async def delete_stock_data(stock_code: str):
    """
    删除指定stock代码的数据
    
    Args:
        stock_code: stock代码
        
    Returns:
        删除结果
    """
    logger.info(f"Received DELETE request for stock data: {stock_code}")
    
    # 模拟删除逻辑
    return {"message": f"Stock data for {stock_code} deleted successfully"}

# 全局异常处理器
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """处理HTTP异常"""
    return Response(
        content=json.dumps({
            "error": exc.detail,
            "status_code": exc.status_code
        }),
        status_code=exc.status_code,
        media_type="application/json"
    )

# 请求中间件
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """记录所有请求的日志中间件"""
    logger.info(f"Incoming request: {request.method} {request.url}")
    
    try:
        response = await call_next(request)
        logger.info(f"Response status: {response.status_code}")
        return response
    except Exception as e:
        logger.error(f"Request failed with error: {str(e)}")
        raise e

# 应用启动和关闭事件
@app.on_event("startup")
async def startup_event():
    """应用启动时执行的事件"""
    logger.info("Stock data Service is starting up...")
    # 可以在这里初始化数据库连接、加载配置等

@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时执行的事件"""
    logger.info("Stock data Service is shutting down...")
    # 可以在这里关闭数据库连接、清理资源等

# 主函数，用于直接运行服务器
def run_server(host="127.0.0.1", port=20000, reload=False):
    """
    运行FastAPI服务器
    
    Args:
        host: 主机地址
        port: 端口号
        reload: 是否启用热重载（开发模式）
    """
    logging.info("server running!")
    uvicorn.run(
        "network.server:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )

if __name__ == "__main__":
    # 直接运行服务器
    run_server(reload=True)