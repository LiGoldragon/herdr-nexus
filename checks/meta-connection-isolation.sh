#!/usr/bin/env bash
set -euo pipefail

package=${1:?package}
backend=${2:?backend executable}
base=$(mktemp -d /tmp/hmi.XXXXXX)
pair_pid=

cleanup() {
  set +e
  [[ -n $pair_pid ]] && kill "$pair_pid" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$base"
}
trap cleanup EXIT

export XDG_RUNTIME_DIR=$base/run
export HOME=$base/home
directory=$XDG_RUNTIME_DIR/metaflow/one
state=$HOME/.local/state/metaflow/one
mkdir -p "$directory" "$state"
place="{ one $directory $state }"
"$package/bin/herdr-pair" "$place" >"$base/pair.log" 2>&1 &
pair_pid=$!
for _ in $(seq 1 200); do
  [[ -S $directory/herdr-meta.sock && -S $directory/herdr-meta-translator.sock ]] && break
  sleep .02
done
if [[ ! -S $directory/herdr-meta.sock || ! -S $directory/herdr-meta-translator.sock ]]; then
  cat "$base/pair.log" >&2
  exit 1
fi

# A client which is not the captured translator is one failed connection, not
# a failed privileged listener or pair.
python3 - "$directory/herdr-meta.sock" <<'PY'
import socket, sys
s = socket.socket(socket.AF_UNIX)
s.connect(sys.argv[1])
s.close()
PY
"$package/bin/herdr-nexus-meta" one 'Configure.Polling.25' \
  | grep -q Configured

# The text front likewise bounds malformed and partially-progressed frames to
# their own connections.  The partial body exceeds the deployment progress bound.
python3 - "$directory/herdr-meta-translator.sock" <<'PY'
import socket, sys
s = socket.socket(socket.AF_UNIX)
s.connect(sys.argv[1])
s.sendall(b'\x00\x00\x00\x01x')
s.close()
PY
"$package/bin/herdr-nexus-meta" one 'Configure.StartTimeout.1000' \
  | grep -q Configured
python3 - "$directory/herdr-meta-translator.sock" <<'PY'
import socket, sys, time
s = socket.socket(socket.AF_UNIX)
s.connect(sys.argv[1])
s.sendall(b'\x00\x00\x00\x20x')
time.sleep(70.25)
s.close()
PY
"$package/bin/herdr-nexus-meta" one 'Configure.FrameCapacity.8388608' \
  | grep -q Configured

"$package/bin/herdr-nexus-meta" one \
  "Configure.HerdrPath.$backend" | grep -q Configured
"$package/bin/herdr-nexus-meta" one 'Configure.CallTimeout.1000' | grep -q Configured
"$package/bin/herdr-nexus-meta" one 'Configure.OutputLimit.1048576' | grep -q Configured
"$package/bin/herdr-nexus-meta" one 'Configure.TextBudget.{ 1048576 64 }' | grep -q Ready
"$package/bin/herdr-nexus-meta" one 'Configure.SubscriberCapacity.64' | grep -q Configured
"$package/bin/herdr-nexus-meta" one 'Configure.TransportTimeout.1000' | grep -q Ready

# The ordinary path on this same pair still accepts and answers a typed call.
"$package/bin/herdr-nexus" one 'List.smoke' >"$base/ordinary.out"
grep -Eq 'Listed|Refused|Unavailable' "$base/ordinary.out"
kill -0 "$pair_pid"

echo META_CONNECTION_ISOLATION_OK
