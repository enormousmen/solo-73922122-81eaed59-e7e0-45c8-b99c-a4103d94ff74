#!/usr/bin/env bash
# Demo: shared diagnostic SQL service.
# Prereq: service running (docker compose up, or local uvicorn) at $BASE.
set -euo pipefail
BASE="${BASE:-http://localhost:8000}"
JSON='content-type: application/json'

pp() { python3 -m json.tool 2>/dev/null || cat; }
jid() { python3 -c "import json,sys;print(json.load(sys.stdin)['job_id'])"; }

echo "=== 1) submit a 3-statement job; statements run in order, autocommitted ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": [
    "CREATE TEMP TABLE demo_t (a int)",
    "INSERT INTO demo_t VALUES (1),(2),(3)",
    "SELECT sum(a) AS total FROM demo_t"
  ]}' | jid)
echo "job_id=$JOB"
sleep 1
curl -s "$BASE/jobs/$JOB" | pp

echo
echo "=== 2) session state does NOT leak to the next job on the same connection ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": [
    "SET application_name = '"'"'leaky'"'"'",
    "CREATE TEMP TABLE leak_t (x int)",
    "SELECT pg_advisory_lock(99)",
    "SELECT 1"
  ]}' | jid)
sleep 1
CONN=$(curl -s "$BASE/jobs/$JOB" | python3 -c "import json,sys;print(json.load(sys.stdin)['connection_id'])")
echo "dirty job ran on connection $CONN; submitting a check job (both conns may serve it;"
echo "run the demo twice or occupy the other conn to force the same one)"
JOB2=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": [
    "SHOW application_name",
    "SELECT count(*) AS temp_objects FROM pg_class WHERE relnamespace = pg_my_temp_schema()",
    "SELECT pg_try_advisory_lock(99) AS lock_was_free"
  ]}' | jid)
sleep 1
curl -s "$BASE/jobs/$JOB2" | pp

echo
echo "=== 3) cancel a running job; cancel is accepted immediately ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": ["SELECT 1", "SELECT pg_sleep(60)", "SELECT 3"]}' | jid)
sleep 1
echo "-- cancel with a 3s delayed send (test hook delay_ms); the API answers at once:"
time curl -s -XPOST "$BASE/jobs/$JOB/cancel" -H "$JSON" -d '{"delay_ms": 3000}' | pp
echo "-- the running statement is interrupted once the cancel is sent (~3s),"
echo "-- statement 3 is skipped, statement 1 stays committed:"
sleep 5
curl -s "$BASE/jobs/$JOB" | pp

echo
echo "=== 4) slow cleanup (test hook) does not block the other connection ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": ["SELECT 1"],
  "options": {"cleanup_delay_ms": 6000}}' | jid)
sleep 2
echo "-- pool while connection cleans up (state=cleaning):"
curl -s "$BASE/pool" | pp
JOB2=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT 2 AS still_fast"]}' | jid)
sleep 1
echo "-- a job on the OTHER connection finished meanwhile:"
curl -s "$BASE/jobs/$JOB2" | pp

echo
echo "=== 4b) statement hand-off race: cancel in the inter-statement gap ==="
echo "-- 4s gap hook between statements; cancel after S1 finishes, before S2 starts"
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": ["SELECT 1 AS s1", "CREATE TABLE public.demo_gap_xyz (id int)"],
  "options": {"statement_gap_delay_ms": 4000}}' | jid)
# poll until S1 is ok, then cancel while the worker idles in the gap
python3 - "$BASE" "$JOB" <<'PY'
import json, sys, time, urllib.request
base, jid = sys.argv[1], sys.argv[2]
def call(m, p, b=None):
    d = json.dumps(b).encode() if b is not None else None
    r = urllib.request.Request(base + p, data=d, method=m,
                               headers={"content-type": "application/json"})
    with urllib.request.urlopen(r, timeout=30) as x:
        return json.loads(x.read())
deadline = time.monotonic() + 10
while call("GET", f"/jobs/{jid}")["statements"][0]["status"] != "ok":
    time.sleep(0.05)
print("S1 ok; cancelling while the connection is idle in the gap...")
print(json.dumps(call("POST", f"/jobs/{jid}/cancel", {"delay_ms": 500}), indent=2))
PY
sleep 2
echo "-- S2 must be skipped (never sent), job cancelled, no table created:"
curl -s "$BASE/jobs/$JOB" | pp
curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": ["SELECT to_regclass('"'"'public.demo_gap_xyz'"'"') AS table_exists"]}' \
  | jid | while read j; do sleep 1; curl -s "$BASE/jobs/$j" | pp; done

echo
echo "=== 5) late cancel after natural finish is a no-op (409) ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT 1"]}' | jid)
sleep 1
curl -s -o /dev/null -w "cancel after finish -> HTTP %{http_code}\n" \
  -XPOST "$BASE/jobs/$JOB/cancel" -H "$JSON" -d '{}'

echo
echo "=== 6) explicit transaction control is rejected (422) ==="
curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["BEGIN", "SELECT 1"]}' | pp
