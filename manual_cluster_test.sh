#!/usr/bin/env bash
# todu 真实 TiDB 集群手动测试脚本（交互式，每步暂停确认）
#
# 依赖: tiup, mysql 客户端
# 流程: 起集群 -> 建表写数据 -> 误删 -> 逼 GC 跑过 -> 停集群 -> 拷数据 ->
#       todu 恢复并与预期 diff
set -euo pipefail

TODU="$(cd "$(dirname "$0")" && pwd)/target/release/todu"
WORK=/tmp/todu-cluster
TIKV_DATA="$WORK/tikv-data"
DB_PORT=4000
TAG=todu-manual

step() { echo; echo "=================================================================="; echo "== $1"; echo "=================================================================="; }
pause() { read -r -p ">>> 按回车继续..." _; }

[ -x "$TODU" ] || { echo "先编译: cargo build --release"; exit 1; }
command -v tiup >/dev/null || { echo "缺 tiup: curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh"; exit 1; }
command -v mysql >/dev/null || { echo "缺 mysql 客户端 (brew install mysql-client)"; exit 1; }

mkdir -p "$WORK"
rm -rf "$TIKV_DATA" "$WORK/expected.txt" "$WORK/recovered*"

step "1/7  生成 TiKV 配置（显式数据目录，集群停掉后数据归你）"
cat > "$WORK/tikv.toml" <<EOF
[server]
addr = "127.0.0.1:20160"
[storage]
data-dir = "$TIKV_DATA"
[pd]
endpoints = ["127.0.0.1:2379"]
EOF
cat "$WORK/tikv.toml"
pause

step "2/7  启动最小集群 (1 TiDB : 1 TiKV : 1 PD)"
tiup playground v8.5.0 --tag "$TAG" --db 1 --kv 1 --pd 1 --without-monitor \
    --kv.config "$WORK/tikv.toml" > "$WORK/playground.log" 2>&1 &
PG_PID=$!
echo "等待集群就绪（最多 90s）..."
for i in $(seq 1 90); do
  if mysql -h 127.0.0.1 -P $DB_PORT -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo "集群已就绪 (playground.log 在 $WORK/playground.log)"; break
  fi
  [ "$i" = 90 ] && { echo "集群启动失败，看 $WORK/playground.log"; exit 1; }
  sleep 1
done
pause

step "3/7  建表 + 写入 3 行 + 存一份预期答案"
mysql -h 127.0.0.1 -P $DB_PORT -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.t1;
CREATE TABLE test.t1 (
  id INT PRIMARY KEY,
  name VARCHAR(64),
  amount DOUBLE,
  created DATETIME(3),
  extra VARCHAR(64)
);
INSERT INTO test.t1 VALUES
  (1,'alice',10.5,'2026-10-12 10:30:00.500','v1-h1'),
  (2,'bob',20,'2026-10-12 10:30:00.500','v1-h2'),
  (3,'carol',30.25,'2026-10-12 10:30:00.500','v1-h3');
SQL
mysql -h 127.0.0.1 -P $DB_PORT -u root -N -e \
  "SELECT id,name,amount,DATE_FORMAT(created,'%Y-%m-%d %H:%i:%s.%f'),extra FROM test.t1 ORDER BY id" \
  | tee "$WORK/expected.txt"
echo "（以上是预期答案，稍后和 todu 恢复结果 diff）"
pause

step "4/7  模拟事故: UPDATE bob / DELETE carol，然后逼 GC 跑过"
mysql -h 127.0.0.1 -P $DB_PORT -u root <<'SQL'
UPDATE test.t1 SET name='bob-updated', amount=99.5 WHERE id=2;
DELETE FROM test.t1 WHERE id=3;
SET GLOBAL tidb_gc_life_time = '30s';
SET GLOBAL tidb_gc_run_interval = '30s';
SQL
echo "事故后表内容:"
mysql -h 127.0.0.1 -P $DB_PORT -u root -e "SELECT * FROM test.t1"
echo
echo -n "等待 GC（观察 safe point 前移，最长 120s）: "
SP0=$(mysql -h 127.0.0.1 -P $DB_PORT -u root -N -e \
  "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='tikv_gc_safe_point'" 2>/dev/null || echo 0)
for i in $(seq 1 24); do
  sleep 5
  SP=$(mysql -h 127.0.0.1 -P $DB_PORT -u root -N -e \
    "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='tikv_gc_safe_point'" 2>/dev/null || echo 0)
  echo -n "."
  if [ "${SP:-0}" != "${SP0:-0}" ] && [ "${SP:-0}" != "0" ] && [ "$i" -ge 2 ]; then
    echo; echo "GC safe point 已前移: $SP0 -> $SP"; break
  fi
done
echo "此时真实表: 只有 alice 和 bob-updated（carol 已删）"
pause

step "5/7  停集群 + 拷贝 TiKV 数据目录（停了就不会再 compaction）"
kill $PG_PID 2>/dev/null || true
sleep 5
pkill -f "tikv.*$TAG" 2>/dev/null || true
sleep 3
cp -r "$TIKV_DATA" "$TIKV_DATA-copy"
echo "数据已拷贝到 $TIKV_DATA-copy"
ls "$TIKV_DATA-copy/db" | head -8
pause

step "6/7  todu 恢复"
DBDIR="$TIKV_DATA-copy/db"
echo "--- 6.1 meta: 表名 -> tableID ---"
"$TODU" meta --db "$DBDIR" || true
echo
echo "--- 6.2 整库模式: 事故后还活着的行 ---"
"$TODU" recover --db "$DBDIR" --table test.t1 --format csv
echo
echo "--- 6.3 取证模式 + 时间旅行到建表后/事故前 ---"
SST=$(ls "$DBDIR"/*.sst | paste -sd, -)
"$TODU" recover --sst "$SST" --table test.t1 \
  --as-of "$(date -v-10M '+%Y-%m-%d %H:%M:%S')" --format csv | tee "$WORK/recovered.txt"
echo
echo "--- 6.4 browse: bob 的两个版本 + carol 的 Delete 标记 ---"
"$TODU" browse --sst "$SST" --table-id "$("$TODU" meta --db "$DBDIR" | grep -oP 'id=\K[0-9]+' | head -1)" 2>/dev/null | head -20 || \
  "$TODU" browse --sst "$SST" --table test.t1 2>/dev/null | head -20 || true
pause

step "7/7  验证: 恢复出的 3 行 == 建表时的预期答案"
if diff <(tail -n +2 "$WORK/recovered.txt" | sed "s/'//g" | sort) \
        <(sort "$WORK/expected.txt") >/dev/null 2>&1; then
  echo "✅ PASS: todu 从真实集群恢复出了全部 3 行原始数据（含被 GC + DELETE 的 carol）"
else
  echo "对比恢复结果与预期（注意浮点/时间格式可能有表示差异）:"
  echo "--- recovered:"; cat "$WORK/recovered.txt"
  echo "--- expected:";  cat "$WORK/expected.txt"
  echo "（格式对不上但数据一致也算 PASS；行值是 hex 的话把输出发出来分析）"
fi

echo
echo "清理: tiup clean $TAG && rm -rf $WORK"
echo "（先别急着删，可以自己多玩玩 browse/decode-row）"
