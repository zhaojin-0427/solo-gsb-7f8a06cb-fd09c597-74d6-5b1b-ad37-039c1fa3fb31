#!/usr/bin/env bash
# 快速冒烟：提交 -> 读取 -> 幂等重试 -> 单节点演练 -> 修复
# 前提：docker compose up -d --build 已启动（协调端在 8000）
set -euo pipefail

BASE=${BASE:-http://localhost:8000}
NODE3=${NODE3:-http://localhost:8003}
OBJ="smoke-$RANDOM"
KEY="key-$RANDOM"

echo "== 1. 提交新对象（期望版本 0）"
curl -sf -X PUT "$BASE/objects/$OBJ" \
  -H "X-Request-Key: $KEY" -H "X-Expected-Version: 0" \
  --data-binary "hello shard world" | tee /tmp/smoke.put.json
echo

echo "== 2. 读取并比对字节"
curl -sf "$BASE/objects/$OBJ" -o /tmp/smoke.out
[ "$(cat /tmp/smoke.out)" = "hello shard world" ] && echo "bytes OK"

echo "== 3. 同键同内容重试 -> 返回原版本（replayed: true）"
curl -sf -X PUT "$BASE/objects/$OBJ" \
  -H "X-Request-Key: $KEY" -H "X-Expected-Version: 0" \
  --data-binary "hello shard world"
echo

echo "== 4. 过期期望版本 -> 409"
curl -s -o /dev/null -w "%{http_code}\n" -X PUT "$BASE/objects/$OBJ" \
  -H "X-Request-Key: other-$RANDOM" -H "X-Expected-Version: 0" \
  --data-binary "whatever"

echo "== 5. node3 开启演练（首次存片后 503），再提交 v2"
curl -sf -X POST "$NODE3/admin/drill" -H 'Content-Type: application/json' \
  -d '{"fail_after_first_store": true}'
echo
curl -sf -X PUT "$BASE/objects/$OBJ" \
  -H "X-Request-Key: v2-$KEY" -H "X-Expected-Version: 1" \
  --data-binary "version two bytes"
echo
curl -sf "$BASE/objects/$OBJ" -o /tmp/smoke.out2
[ "$(cat /tmp/smoke.out2)" = "version two bytes" ] && echo "read with node3 down OK"

echo "== 6. 关闭演练并修复（补齐 node3 上 v2 的片）"
curl -sf -X POST "$NODE3/admin/drill" -H 'Content-Type: application/json' \
  -d '{"fail_after_first_store": false}'
echo
curl -sf -X POST "$BASE/objects/$OBJ/repair"
echo
echo "SMOKE OK"
