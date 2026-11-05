#!/usr/bin/env bash
set -euo pipefail

package=${1:?package}
base=$(mktemp -d /tmp/hpr.XXXXXX)
launchers=()

cleanup() {
  set +e
  for launcher in "${launchers[@]:-}"; do
    [[ -n $launcher ]] && kill "$launcher" 2>/dev/null
  done
  wait 2>/dev/null
  rm -rf "$base"
}
trap cleanup EXIT

start_pair() {
  local generation=$1
  export XDG_RUNTIME_DIR=$base/$generation/run
  export HOME=$base/$generation/home
  directory=$XDG_RUNTIME_DIR/metaflow/one
  state=$HOME/.local/state/metaflow/one
  mkdir -p "$directory" "$state"
  place="{ one $directory $state }"
  "$package/bin/herdr-pair" "$place" >"$base/$generation.log" 2>&1 &
  pair_pid=$!
  launchers+=("$pair_pid")
  child_pids=()
  for _ in $(seq 1 100); do
    read -r -a child_pids <<<"$(cat "/proc/$pair_pid/task/$pair_pid/children" 2>/dev/null || true)"
    [[ ${#child_pids[@]} -eq 3 && -S $directory/herdr-nexus-meta-translator.sock ]] && break
    sleep .05
  done
  if [[ ${#child_pids[@]} -ne 3 || ! -S $directory/herdr-nexus-meta-translator.sock ]]; then
    cat "$base/$generation.log" >&2
    return 1
  fi
  "$package/bin/herdr-nexus-meta" one 'Configure.Polling.100' \
    | grep -q Configured
}

stop_one_and_require_siblings_exit() {
  local first=${child_pids[0]}
  local -a siblings=("${child_pids[@]:1}")
  kill "$first"
  if wait "$pair_pid"; then
    echo 'paired launcher unexpectedly succeeded after a child exited' >&2
    return 1
  fi
  for sibling in "${siblings[@]}"; do
    for _ in $(seq 1 100); do
      kill -0 "$sibling" 2>/dev/null || break
      sleep .02
    done
    if kill -0 "$sibling" 2>/dev/null; then
      echo "paired launcher left sibling $sibling running" >&2
      return 1
    fi
  done
}

start_pair first
old_children=("${child_pids[@]}")
stop_one_and_require_siblings_exit

start_pair second
for old in "${old_children[@]}"; do
  for fresh in "${child_pids[@]}"; do
    [[ $old != "$fresh" ]]
  done
done
stop_one_and_require_siblings_exit

echo PAIRED_RELAUNCH_OK
