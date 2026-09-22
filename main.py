from fastapi import FastAPI

from fleet.api import create_app

# 保持模块级 ``app`` 与既有启动方式（uvicorn main:app / fastapi run）可用。
app: FastAPI = create_app()
