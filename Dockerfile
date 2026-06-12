FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md config.yml.example ./
COPY src ./src
RUN pip install --no-cache-dir .

# 预装 DuckDB 扩展，运行时无需联网下载。Pre-install DuckDB extensions so no network download is needed at runtime.
RUN python -c "import duckdb; conn = duckdb.connect(); \
    [conn.execute(f'INSTALL {e}') for e in ('mysql', 'httpfs', 'icu')]"

ENV EBB_CONFIG=/etc/ebb/config.yml

ENTRYPOINT ["ebb"]
CMD ["serve"]
