#!/usr/bin/env bash
set -euo pipefail

package=${1:?package}
base=$(mktemp -d)
pair=
cleanup() {
  set +e
  [[ -n $pair ]] && kill "$pair" 2>/dev/null
  wait "$pair" 2>/dev/null
  rm -rf "$base"
}
trap cleanup EXIT

directory=$base/main
workspace=$base/workspace
mkdir "$directory" "$workspace"
registry=$base/registry.datom
printf '[ { one %s } ]\n' "$directory" >"$registry"

{
printf '#!%s\n' "$(command -v bash)"
cat <<'EOF'
set -euo pipefail
case "$3:$4" in
  workspace:create)
    printf '%s\n' '{"id":"cli:workspace:create","result":{"type":"workspace_created","workspace":{},"tab":{},"root_pane":{"pane_id":"w:p","terminal_id":"t","workspace_id":"w","tab_id":"tab","focused":true,"agent_status":"unknown","revision":1}}}'
    ;;
  agent:start)
    sleep .20
    printf '%s\n' '{"id":"cli:agent:start","result":{"type":"agent_started","agent":{"terminal_id":"t","name":"probe","agent":"codex","agent_status":"idle","workspace_id":"w","tab_id":"tab","pane_id":"w:p","focused":true,"interactive_ready":true,"revision":2},"argv":["codex"]}}'
    ;;
  *) exit 19 ;;
esac
EOF
} >"$base/herdr-fixture"
chmod +x "$base/herdr-fixture"

"$package/bin/herdr-pair" "$registry" one \
  "$package/bin/herdr-nexus-daemon" "$package/bin/herdr-translator" \
  50 8388608 1048576 64 >"$base/pair.log" 2>&1 &
pair=$!
for _ in $(seq 1 100); do
  [[ -S $directory/herdr-meta-translator.sock && -S $directory/herdr-meta.sock ]] && break
  sleep .02
done
[[ -S $directory/herdr-meta-translator.sock ]]
[[ -S $directory/herdr-meta.sock ]]

meta="$package/bin/herdr-nexus-meta"
public="$package/bin/herdr-nexus"
"$meta" "$registry" one "Configure.HerdrPath.$base/herdr-fixture" >/dev/null
"$meta" "$registry" one 'Configure.CallTimeout.50' >/dev/null
"$meta" "$registry" one 'Configure.OutputLimit.1048576' >/dev/null
"$meta" "$registry" one 'Configure.TextBudget.{ 1048576 64 }' >/dev/null
"$meta" "$registry" one 'Configure.StartTimeout.1000' >/dev/null
"$meta" "$registry" one 'Configure.Polling.100' >/dev/null
"$meta" "$registry" one 'Configure.FrameCapacity.8388608' >/dev/null
"$meta" "$registry" one 'Configure.SubscriberCapacity.8' >/dev/null
"$meta" "$registry" one 'Configure.TransportTimeout.50' | grep -q Ready

"$public" "$registry" one "Create.{ main $workspace [] }" | grep -q 'Created.Created'
began=$(date +%s%3N)
"$public" "$registry" one 'Start.{ { main w:p } { probe Codex Configured [] } label }' \
  | grep -q 'Started.Done'
ended=$(date +%s%3N)
(( ended - began >= 150 ))

"$meta" "$registry" one 'Configure.StartTimeout.40' >/dev/null
"$public" "$registry" one 'Start.{ { main w:p } { probe Codex Configured [] } label }' \
  | grep -q Uncertain

echo DELAYED_START_OK
