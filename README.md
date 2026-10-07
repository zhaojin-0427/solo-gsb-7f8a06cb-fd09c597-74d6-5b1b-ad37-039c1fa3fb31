# shardstore — 三节点 2-of-3 异或纠删对象存储

素材归档服务：一台存储节点停机时仍可读写原件。客户端按名称提交 1～65536 字节原文，
协调端把原文拆成三片（任意两片可还原），分存三台带独立持久卷的分片节点；
协调端只保存元数据（SQLite），不落地任何可恢复的整件内容。

## 架构

```
            ┌──────────────┐
  客户端 ──▶│  协调端 :8000 │  只存元数据：原长、整件/各片 SHA-256、版本、请求键、已确认副本
            └──────┬───────┘
        ┌──────────┼──────────┐
        ▼          ▼          ▼
   node1 :8001  node2 :8002  node3 :8003     （容器内均为 8000，宿主映射 8001/8002/8003）
   片0          片1          片2             各自独立持久卷 node{1,2,3}-data
```

**编码方案**（`app/common.py`）：原长 `L`，半长 `half = ceil(L/2)`。

- 片 0 = 前半片；片 1 = 后半片右侧补零至 `half`；片 2 = 片 0 与片 1 的逐字节异或。
- 任意两片还原准确字节（按原长截断）；任一片缺损可由另两片推导。
- 每个版本记录：原长、整件 SHA-256、三片各自的 SHA-256。

**关键语义**

| 需求 | 机制 |
| --- | --- |
| 发布条件 | ≥2 片确认持久化（节点 fsync 后应答）且期望版本仍是当前版，才发布递增版本（CAS） |
| 单节点离线 | 2-of-3  quorum：提交、读取照常 |
| 不足两片 | 提交/读取返回 503；提交失败不改变已发布版本 |
| 读取一致性 | GET 固定请求开始时的已发布版本；分片按 `(name, version, idx)` 不可变寻址，并发覆盖不会混片 |
| 可信片 | 仅接受与已发布摘要一致的片；还原后再校验整件摘要，不符返回 503 |
| 幂等 | 同名同键同内容重试返回原版本；未发布请求同键续交；异内容或过期版本返回 409 |
| 崩溃续交 | 节点存片后丢失回执 / 协调端重启：pending 版本持久化在 SQLite，重试时先 HEAD 查询节点已有片再补缺，复用原版本号，绝不产生第二版本 |
| 修复 | `POST /objects/{name}/repair` 按当前版本补齐缺损片；写每片前重查版本，版本推进即放弃（409），旧片落不到新版本 |
| 演练开关 | 节点可设置「首次存片后返回 503」（环境变量或 `/admin/drill`） |
| 非法输入 / 不存在 | 422 / 404 |

## 启动

```bash
docker compose up -d --build
```

| 服务 | 宿主端口 | 说明 |
| --- | --- | --- |
| coordinator | 8000 | 对象 API（客户端入口） |
| node1 / node2 / node3 | 8001 / 8002 / 8003 | 分片节点，各挂独立卷 `node{1,2,3}-data` |

停止并清空数据：`docker compose down -v`

## curl 示例

### 提交对象（新对象期望版本为 0）

```bash
curl -X PUT http://localhost:8000/objects/hello \
  -H 'X-Request-Key: 7f3c9a2e-0001' \
  -H 'X-Expected-Version: 0' \
  --data-binary 'hello erasure world'
# 201 {"name":"hello","version":1,"size":19,"sha256":"...","state":"published","replayed":false}
```

提交二进制文件（≤65536 字节）：

```bash
curl -X PUT http://localhost:8000/objects/clip.bin \
  -H "X-Request-Key: $(uuidgen)" -H 'X-Expected-Version: 0' \
  --data-binary @clip.bin
```

### 覆盖（期望版本 = 当前已发布版本）

```bash
curl -X PUT http://localhost:8000/objects/hello \
  -H 'X-Request-Key: 7f3c9a2e-0002' -H 'X-Expected-Version: 1' \
  --data-binary 'hello v2'
# 201 version=2；期望版本过期或同键异内容 -> 409
```

### 读取（任意两片还原，响应头带版本与整件摘要）

```bash
curl -i http://localhost:8000/objects/hello -o hello.out
# X-Object-Version: 2 / X-Object-Sha256: ... / X-Object-Size: 8
```

### 元数据 / 修复

```bash
curl http://localhost:8000/objects/hello/meta
curl -X POST http://localhost:8000/objects/hello/repair
```

### 故障演练

```bash
# node3 开启「首次存片后 503」；也可用环境变量 DRILL_FAIL_AFTER_FIRST_STORE=1 预置
curl -X POST http://localhost:8003/admin/drill \
  -H 'Content-Type: application/json' -d '{"fail_after_first_store": true}'

# 此时提交/读取仍成功（2-of-3）；再停一台则提交 503 且已发布版本不变
curl -X POST http://localhost:8003/admin/drill \
  -H 'Content-Type: application/json' -d '{"fail_after_first_store": false}'   # 恢复
curl -X POST http://localhost:8000/objects/hello/repair                        # 补齐 node3 缺损片
```

一键冒烟（提交→读取→幂等→演练→修复）：

```bash
./scripts/smoke.sh
```

## API 一览

协调端 `:8000`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| PUT | `/objects/{name}` | 提交。头：`X-Request-Key`、`X-Expected-Version`；体为原文（1..65536 字节） |
| GET | `/objects/{name}` | 读取当前已发布版本（请求开始时固定） |
| GET | `/objects/{name}/meta` | 当前版本元数据：原长、整件及各片摘要、已确认副本 |
| POST | `/objects/{name}/repair` | 按当前版本补齐缺损片 |
| GET | `/healthz` | 健康检查 |

分片节点 `:8001~8003`（容器内 8000）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| PUT | `/shards/{name}/{version}/{idx}` | 存片（头 `X-Shard-Sha256`）。同标识同摘要幂等重写；异摘要 409 |
| GET / HEAD | `/shards/{name}/{version}/{idx}` | 取片 / 查询（头返回已记录摘要与实际摘要） |
| GET / POST | `/admin/drill` | 查看 / 设置演练开关 |
| GET | `/healthz` | 健康检查 |

**状态码**：`200/201` 成功（201=新发布，200=读取或幂等重放）；`404` 对象/分片不存在；
`409` 期望版本过期、同键异内容、节点异摘要写、修复期间版本被推进；
`422` 非法输入（名称、请求键、期望版本、长度越界、分片摘要与内容不符）；
`503` 不足两片可信/持久化、整件摘要校验失败、节点演练中。

## 本地开发（不用 Docker）

```bash
pip install -r requirements.txt

DATA_DIR=/tmp/n1 NODE_ID=node1 uvicorn app.node:app --port 8001 &
DATA_DIR=/tmp/n2 NODE_ID=node2 uvicorn app.node:app --port 8002 &
DATA_DIR=/tmp/n3 NODE_ID=node3 uvicorn app.node:app --port 8003 &
DB_PATH=/tmp/coord.db NODES=http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003 \
  uvicorn app.coordinator:app --port 8000 &
```

端到端场景测试（幂等、CAS、故障演练、重启续交、损坏修复、并发覆盖，共 50+ 断言）：

```bash
NODE_DIRS=/tmp/n1,/tmp/n2,/tmp/n3 COORD_DB=/tmp/coord.db \
COORD_PID_FILE=<协调端pid文件> python3 tests/e2e_test.py
```

## 目录结构

```
app/common.py       2-of-3 异或编码、SHA-256、输入校验
app/coordinator.py  协调端：元数据（SQLite）、发布 CAS、读取还原、修复
app/node.py         分片节点：不可变存片、摘要侧车、演练开关
tests/e2e_test.py   端到端场景测试
scripts/smoke.sh    curl 冒烟脚本
Dockerfile          单一镜像，compose 中以 command 区分角色
docker-compose.yml  协调端 + 3 节点（各自独立持久卷）
```
