"""HTTP 服务：

- 查询 API（可选启用，对外）：POST /query，Bearer Token 认证；
- 管理端点（始终启用，只监听回环地址）：GET /admin/jobs，供 ebb ps 读取。

两者是独立的 FastAPI 应用，分别监听不同地址。
"""

from __future__ import annotations

import secrets

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from .config import Config
from .logs import log_error
from .queryservice import QueryRejected, QueryTimeout, run_query
from .registry import Registry


class QueryRequest(BaseModel):
    sql: str
    max_rows: int | None = None


def build_query_app(config: Config) -> FastAPI:
    app = FastAPI(title="ebb query api", docs_url=None, redoc_url=None, openapi_url=None)
    bearer = HTTPBearer(auto_error=False)

    def authed(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    ) -> None:
        if credentials is None or not any(
            secrets.compare_digest(credentials.credentials, key)
            for key in config.query_api.api_keys
        ):
            raise HTTPException(status_code=401, detail="无效的 API Key")

    @app.post("/query", dependencies=[Depends(authed)])
    def query(req: QueryRequest):
        max_rows = config.query_api.max_rows
        if req.max_rows is not None:
            max_rows = min(req.max_rows, max_rows)
        try:
            result = run_query(config, req.sql, max_rows=max_rows)
        except QueryRejected as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except QueryTimeout as exc:
            raise HTTPException(status_code=408, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 SQL 错误统一 422
            log_error("query", exc=exc)
            raise HTTPException(status_code=422, detail=f"{exc}") from exc
        return {
            "columns": result.columns,
            "rows": result.rows,
            "row_count": result.row_count,
            "truncated": result.truncated,
        }

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    return app


def build_admin_app(registry: Registry) -> FastAPI:
    app = FastAPI(title="ebb admin", docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/admin/jobs")
    def jobs(_: Request):
        return {"jobs": registry.snapshot()}

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    return app
