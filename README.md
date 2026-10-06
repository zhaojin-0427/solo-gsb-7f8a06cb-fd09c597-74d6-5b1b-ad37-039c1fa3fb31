# 对象 HTTP API（2+1 纠删码，单协调端 + 三分片节点）

素材归档系统：一个协调端（FastAPI，只存 **元数据**）和三个分片节点（各自
独立的 SQLite 持久卷）。每个对象被切成两个等长数据片（后片奇数长度时补
零）与一个逐字节 XOR 校验片，**任意两片即可精确还原**。任意一台分片节点
停机时仍可提交和读取。

- 语言 / 框架：Python 3.11、FastAPI、httpx、SQLite、uvicorn
- 纠删码：`shard0` 前片、`shard1` 后片（补零）、`shard2 = shard0 XOR shard1`
- 仲裁：**至少两片** 确认持久化 **且** 期望版本仍为当前版本，才发布递增版本
- 存储分离：协调端数据库中只有原长、整件 SHA-256、各片 SHA-256、版本与
  请求键记录；协调端不落任何对象字节，单拿任何一片都无法还原整件

## 端口

| 服务        | 容器端口 | 宿主机端口 | 持久卷           |
|-------------|---------|-----------|------------------|
| coordinator | 8000    | **8000**  | `coord-data`     |
| node0       | 8100    | **8100**  | `node0-data`     |
| node1       | 8100    | **8101**  | `node1-data`     |
| node2       | 8100    | **8102**  | `node2-data`     |

API 基址：`http://localhost:8000`

## 启动 / 停止

```bash
docker compose build
docker compose up -d
docker compose ps
curl -s http://localhost:8000/health | python3 -m json.tool

docker compose logs -f coordinator
docker compose down            # 保留卷中的数据
docker compose down -v         # 连同持久卷一并删除
```

## 提交对象（PUT）

- 路径：`PUT /objects/{name}`
- 正文：**1～65536 字节**原始字节
- 请求头：
  - `X-Request-Key`：请求键（客户端幂等/续交标识，`[A-Za-z0-9._:-]{1,128}`）
  - `X-Expected-Version`：期望的当前版本；新对象传 `0`，覆盖时传当前版本

```bash
# 新建（期望版本 0）
curl -s -X PUT http://localhost:8000/objects/report.txt \
  -H 'X-Request-Key: req-0001' \
  -H 'X-Expected-Version: 0' \
  --data-binary '素材归档在一台存储节点停机时仍须读写原件' | python3 -m json.tool
# 200 -> {"status":"published","version":1,"write_id":"w1_v1",
#         "durable_shards":[0,1,2], ...}

# 覆盖（期望版本 1）
curl -s -X PUT http://localhost:8000/objects/report.txt \
  -H 'X-Request-Key: req-0002' \
  -H 'X-Expected-Version: 1' \
  --data-binary '更新后的归档内容' | python3 -m json.tool
# 200 -> {"version":2,...}
```

成功发布返回 200，响应头/正文给出 `version` 与 `write_id`。

## 读取对象（GET）

`GET /objects/{name}` 固定返回**请求开始时已发布的版本**；也可显式
`?version=N` 读取历史版本。用任意两片还原，并校验各片 SHA-256、奇偶关系
和整件 SHA-256，任何一步不符返回 **503**。

```bash
curl -si http://localhost:8000/objects/report.txt
# 响应头：X-Object-Version / X-Write-Id / X-Whole-SHA256 / X-Original-Length

curl -s http://localhost:8000/objects/report.txt?version=1   # 读历史版本
curl -s http://localhost:8000/objects                          # 列出对象
curl -s http://localhost:8000/objects/report.txt/versions      # 版本与摘要
```

## 节点停机演练（一台 / 两台）

```bash
docker compose stop node1                 # 停一台：仍可读写
curl -s -X PUT http://localhost:8000/objects/alpha \
  -H 'X-Request-Key: alpha-1' -H 'X-Expected-Version: 0' \
  --data-binary 'one node down is fine'
curl -s http://localhost:8000/objects/alpha

docker compose stop node2                 # 再停一台：只剩一片 -> 503
curl -si -X PUT http://localhost:8000/objects/beta \
  -H 'X-Request-Key: beta-1' -H 'X-Expected-Version: 0' \
  --data-binary 'needs two shards'        # 503，已发布版本不变
curl -si http://localhost:8000/objects/beta  # 404：从未发布过

docker compose start node2                # 节点恢复
# 用同一个请求键续交未发布请求 -> 仍只产生一个版本
curl -s -X PUT http://localhost:8000/objects/beta \
  -H 'X-Request-Key: beta-1' -H 'X-Expected-Version: 0' \
  --data-binary 'needs two shards'        # 200，version=1

# 补回停机期间 node1 缺失的片（按当前版本的已发布摘要修复）
curl -s -X POST http://localhost:8000/objects/alpha/repair | python3 -m json.tool
```

## 幂等与冲突

| 场景                                         | 结果 |
|----------------------------------------------|------|
| 已发布请求同名 + 同键 + 同内容重试           | 200，返回**原版本**（`idempotent_replay:true`） |
| 未发布请求同键续交（含回执丢失、协调端重启） | 继续同一个 `write_id`，不产生第二版本 |
| 同键异内容 / 同键异对象                       | **409** |
| `X-Expected-Version` 已过期（并发覆盖落败）  | **409**，本次提交不发布 |
| 持久片不足两片                               | **503**，已发布版本不变，可用同键重试 |

```bash
# 幂等重试：返回同一个版本
curl -s -X PUT http://localhost:8000/objects/report.txt \
  -H 'X-Request-Key: req-0001' -H 'X-Expected-Version: 0' \
  --data-binary '素材归档在一台存储节点停机时仍须读写原件'

# 同键异内容 -> 409
curl -si -X PUT http://localhost:8000/objects/report.txt \
  -H 'X-Request-Key: req-0001' -H 'X-Expected-Version: 0' \
  --data-binary 'different content'

# 过期版本 -> 409
curl -si -X PUT http://localhost:8000/objects/report.txt \
  -H 'X-Request-Key: req-0003' -H 'X-Expected-Version: 1' \
  --data-binary 'trying against stale version'
```

## 修复（按已发布摘要补齐缺损片）

协调端对**当前版本**做一次快照，用任意两片可信片重算出第三片，并只把缺损/
损坏的槽位写回节点；节点只接受内容哈希等于该槽**已发布摘要**的修复写入。
修复过程中每写一片前都会复查当前版本，版本已前进则放弃写入并返回 **409**，
绝不会用旧版本的片覆盖新版本。

```bash
# 直接损坏 node0 上某版本的片 0（运维接口，仅用于演练）。
# write_id 取自 PUT 成功响应里的 "write_id"（形如 w1_v2）：
curl -s -X POST http://localhost:8100/admin/shards/w1_v2/0/corrupt

curl -s -X POST http://localhost:8000/objects/report.txt/repair | python3 -m json.tool
# {"status":"repaired","repaired_shards":[0], ...}；再次修复 -> "healthy"
```

## 节点演练开关（首次存片后返回 503）

每个分片节点都有一个演练开关：打开后，**首次存片成功后回执变为 503**（片
已落盘），用于模拟"存片后丢失回执"。协调端不会在本次请求内把它算作确认，
但下一次同键续交时会先探测到已存片并继续同一版本。

```bash
# 节点端口直连：8100/8101/8102
curl -s http://localhost:8102/admin/drill                 # {"armed":false}
curl -s -X POST http://localhost:8102/admin/drill \
  -H 'Content-Type: application/json' -d '{"armed":true}'

curl -s -X PUT http://localhost:8000/objects/drill \
  -H 'X-Request-Key: drill-1' -H 'X-Expected-Version: 0' \
  --data-binary 'drill: node2 ack lost after first store'
# node0/node1 两片足够 -> 200；若只武装两台节点 -> 首次 503，同键续交成功

# 同键重试：协调端探测到 node2 已有的片，返回同一版本
curl -s -X PUT http://localhost:8000/objects/drill \
  -H 'X-Request-Key: drill-1' -H 'X-Expected-Version: 0' \
  --data-binary 'drill: node2 ack lost after first store'

curl -s -X POST http://localhost:8102/admin/drill \
  -H 'Content-Type: application/json' -d '{"armed":false}'
```

## 错误码

| 状态码 | 触发情况 |
|--------|----------|
| **422** | 空正文 / 超过 65536 字节 / 名称或请求键非法 / 缺少必需请求头 / 摘要格式错 |
| **404** | 对象或版本不存在 |
| **409** | 同键异内容、期望版本过期（并发覆盖落败）、修复期间版本前进、普通写入撞上不可变槽 |
| **503** | 可信片不足两片、整件摘要不符、片间奇偶关系不符、节点不可达 |

## 本地运行（不使用 Docker）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 三个分片节点（各一个独立目录，等价于三个独立持久卷）
mkdir -p data/n0 data/n1 data/n2
DB_PATH=data/n0/node.db SERVICE_ROLE=node NODE_NAME=node0 PORT=8100 \
  python -m uvicorn app.main:app_factory --factory --port 8100 &
DB_PATH=data/n1/node.db SERVICE_ROLE=node NODE_NAME=node1 PORT=8101 \
  python -m uvicorn app.main:app_factory --factory --port 8101 &
DB_PATH=data/n2/node.db SERVICE_ROLE=node NODE_NAME=node2 PORT=8102 \
  python -m uvicorn app.main:app_factory --factory --port 8102 &

# 协调端
DB_PATH=data/coordinator.db SERVICE_ROLE=coordinator PORT=8000 \
NODE0_URL=http://127.0.0.1:8100 NODE1_URL=http://127.0.0.1:8101 \
NODE2_URL=http://127.0.0.1:8102 \
  python -m uvicorn app.main:app_factory --factory --port 8000
```

## 测试

测试会在 localhost 临时端口上真实启动 1 个协调端 + 3 个节点进程，覆盖：
读写往返（含 1/65535/65536 字节、奇偶数长度）、单节点停机读写、双节点停机
503 且不发布、幂等返回原版本、未发布同键续交、协调端重启续交、同键异内容
与过期版本 409、并发覆盖一胜一败且不混片、GET 版本固定、损坏片拒绝普通覆
写、按摘要修复、修复不覆盖新版本、演练开关、422/404 等。

```bash
pip install -r requirements.txt pytest pytest-asyncio
python -m pytest
```

## 目录结构

```
app/
  coding.py       # 2+1 拆片 / 任意两片还原 / 奇偶校验
  node.py         # 分片节点 FastAPI 应用（不可变槽位、修复、演练开关、SQLite）
  store.py        # 协调端 SQLite 元数据（对象/版本/未发布写入/请求键）
  nodeclient.py   # 协调端 -> 分片节点的 httpx 异步客户端
  coordinator.py  # 对象 HTTP API：提交/读取/修复、仲裁、版本固定
  main.py         # 单镜像入口（SERVICE_ROLE=coordinator|node）
tests/
  test_e2e.py     # 真实 HTTP 进程端到端测试
Dockerfile
docker-compose.yml
requirements.txt
```
