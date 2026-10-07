FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

# 默认启动协调端；分片节点在 Compose 中用 command 覆盖
CMD ["uvicorn", "app.coordinator:app", "--host", "0.0.0.0", "--port", "8000"]
