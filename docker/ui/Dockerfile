# walshadow demo UI image. Deliberately tiny — one FastAPI module and one
# hand-written HTML page, no JS toolchain.
#
# python:3.13-slim (glibc), NOT alpine: pip resolves manylinux wheels for
# psycopg's C driver here. On musl there is no psycopg-binary wheel and the
# build would need gcc + libpq-dev + python3-dev, a ~200MB toolchain layer
# for a 400-line app.
#
# app.py / index.html are COPYed so the image stands alone (plain
# `docker build`, no compose). docker-compose.demo.yml re-mounts both
# read-only over the top, so editing either one is
# `docker compose ... restart ui` — no rebuild.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY app.py index.html ./

EXPOSE 8088
CMD ["python", "-m", "uvicorn", "app:app", \
     "--host", "0.0.0.0", "--port", "8088", "--log-level", "warning"]
