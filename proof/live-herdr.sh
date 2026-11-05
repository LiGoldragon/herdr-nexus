#!/usr/bin/env bash
# Rerunnable installed-front proof. Every path, session and pane is uniquely owned by this run.
set -euo pipefail

package=${1:?usage: live-herdr.sh PACKAGE [HERDR]}
herdr=${2:-/home/li/.nix-profile/bin/herdr}
tag="v1proof-${USER}-$$"
if [[ -n ${PROOF_BASE:-} ]]; then
  base=$PROOF_BASE
  mkdir "$base"
else
  base=$(mktemp -d "/tmp/${tag}.XXXXXX")
fi
export XDG_RUNTIME_DIR=$base/run
export HOME=$base/home
one="$XDG_RUNTIME_DIR/metaflow/one"
two="$XDG_RUNTIME_DIR/metaflow/two"
state_one="$HOME/.local/state/metaflow/one"
state_two="$HOME/.local/state/metaflow/two"
mkdir -p "$one" "$two" "$state_one" "$state_two" "$base/work-one" "$base/work-two"
pids=()
sessions=("${tag}-one" "${tag}-two")
panes=()

cleanup() {
  set +e
  printf 'cleanup owns base=%s sessions=%s pids=%s panes=%s\n' "$base" "${sessions[*]}" "${pids[*]:-}" "${panes[*]:-}" >&2
  for pane in "${panes[@]:-}"; do
    [[ -n "$pane" ]] && "$herdr" --session "${pane%%:*}" pane close "${pane#*:}" >/dev/null 2>&1
  done
  for session in "${sessions[@]}"; do
    "$herdr" --session "$session" workspace close w1 >/dev/null 2>&1
    "$herdr" --session "$session" server stop >/dev/null 2>&1
  done
  for pid in "${pids[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" >/dev/null 2>&1
  done
  wait >/dev/null 2>&1
  if [[ -n ${KEEP_PROOF:-} ]]; then
    printf 'retaining owned fixture directory for diagnosis: %s\n' "$base" >&2
  else
    printf 'deleting owned fixture directory %s\n' "$base" >&2
    rm -rf "$base"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cat > "$base/herdr" <<WRAPPER
#!/usr/bin/env bash
exec "$package/bin/herdr-nexus" "\$@"
WRAPPER
cat > "$base/herdr-meta" <<WRAPPER
#!/usr/bin/env bash
exec "$package/bin/herdr-nexus-meta" "\$@"
WRAPPER
chmod +x "$base/herdr" "$base/herdr-meta"
public="$base/herdr"
meta="$base/herdr-meta"

start_metaflow() {
  local name=$1
  local directory state
  directory=$([[ $name == one ]] && printf %s "$one" || printf %s "$two")
  state=$([[ $name == one ]] && printf %s "$state_one" || printf %s "$state_two")
  "$package/bin/herdr-pair" "{ $name $directory $state }" >"$base/$name-pair.log" 2>&1 &
  pids+=("$!")
  for _ in $(seq 1 100); do
    [[ -S "$directory/herdr-nexus-meta-translator.sock" && -S "$directory/herdr-nexus-meta.sock" ]] && return
    sleep .05
  done
  echo "metaflow $name did not bind bootstrap sockets" >&2
  return 1
}

configure() {
  local name=$1
  # A malformed exchange is isolated; the immediately following valid Set must succeed.
  if "$meta" "$name" NotAQuery >"$base/$name-malformed.out" 2>&1; then
    echo 'malformed meta unexpectedly succeeded' >&2
    return 1
  fi
  "$meta" "$name" "Configure.HerdrPath.$herdr"
  "$meta" "$name" 'Configure.CallTimeout.30000'
  "$meta" "$name" 'Configure.OutputLimit.1048576'
  "$meta" "$name" 'Configure.TextBudget.{ 1048576 64 }' | tee "$base/$name-translator-ready.out"
  "$meta" "$name" 'Configure.StartTimeout.60000'
  "$meta" "$name" 'Configure.Polling.100'
  "$meta" "$name" 'Configure.FrameCapacity.8388608'
  "$meta" "$name" 'Configure.SubscriberCapacity.64'
  "$meta" "$name" 'Configure.TransportTimeout.70000' | tee "$base/$name-core-ready.out"
  grep -q Ready "$base/$name-translator-ready.out"
  grep -q Ready "$base/$name-core-ready.out"
}

start_metaflow one
start_metaflow two
configure one
configure two

for session in "${sessions[@]}"; do
  "$herdr" --session "$session" server >"$base/$session-herdr.log" 2>&1 &
  pids+=("$!")
  for _ in $(seq 1 100); do
    "$herdr" --session "$session" status server >/dev/null 2>&1 && break
    sleep .05
  done
  "$herdr" --session "$session" status server >/dev/null
done

create_one=$($public one "Create.{ ${sessions[0]} $base/work-one [] }")
create_two=$($public two "Create.{ ${sessions[1]} $base/work-two [] }")
printf '%s\n%s\n' "$create_one" "$create_two"
pane_one=$(grep -oE 'w[0-9]+:p[0-9]+' <<<"$create_one" | head -1)
pane_two=$(grep -oE 'w[0-9]+:p[0-9]+' <<<"$create_two" | head -1)
[[ -n "$pane_one" && -n "$pane_two" ]]
panes+=("${sessions[0]}:$pane_one" "${sessions[1]}:$pane_two")

agent="v1proof$$"
"$public" one "Start.{ { ${sessions[0]} $pane_one } { $agent Codex Configured [] } «Mind versionOne Secondary proof» }"
# Overlapping pane IDs in two directories stay isolated; calls progress concurrently.
"$public" one "List.${sessions[0]}" >"$base/list-one.out" & list_one=$!
"$public" two "List.${sessions[1]}" >"$base/list-two.out" & list_two=$!
wait "$list_one" "$list_two"
grep -q "$pane_one" "$base/list-one.out"
grep -q "$pane_two" "$base/list-two.out"
timeout 45 "$public" one "Subscribe.${sessions[0]}" >"$base/subscription.out" & subscription=$!
for _ in $(seq 1 100); do grep -q Subscribed "$base/subscription.out" 2>/dev/null && break; sleep .05; done
grep -q Subscribed "$base/subscription.out"
# A live subscriber must keep using changed settings.
"$meta" one 'Configure.Polling.200' | grep -q Configured
subscription_mark=$(wc -c <"$base/subscription.out")
"$public" one "Prompt.{ { ${sessions[0]} $pane_one } V1HERDRNONCE }"
# The command must begin in an empty input after the prompted turn becomes
# busy and then idle; otherwise key injection could append to that prompt.
for _ in $(seq 1 150); do
  tail -c "+$((subscription_mark + 1))" "$base/subscription.out" | grep -q Busy && break
  sleep .1
done
tail -c "+$((subscription_mark + 1))" "$base/subscription.out" | grep -q Busy
for _ in $(seq 1 300); do
  tail -c "+$((subscription_mark + 1))" "$base/subscription.out" | grep -q Idle && break
  sleep .1
done
tail -c "+$((subscription_mark + 1))" "$base/subscription.out" | grep -q Idle
"$public" one "Press.{ { ${sessions[0]} $pane_one } [ / q u i t ] }"
sleep .2
"$public" one "Press.{ { ${sessions[0]} $pane_one } [ Enter ] }"
for _ in $(seq 1 150); do grep -q Dead "$base/subscription.out" 2>/dev/null && break; sleep .2; done
grep -q Dead "$base/subscription.out"
# The pane itself remains present when harness loss is reported.
"$herdr" --session "${sessions[0]}" pane get "$pane_one" >"$base/pane-after-death.txt"
grep -q "$pane_one" "$base/pane-after-death.txt"
kill "$subscription" >/dev/null 2>&1 || true
wait "$subscription" >/dev/null 2>&1 || true

"$public" one "Close.{ ${sessions[0]} $pane_one }"
"$public" two "Close.{ ${sessions[1]} $pane_two }"
panes=()
echo "LIVE_HERDR_PROOF_OK $base"
