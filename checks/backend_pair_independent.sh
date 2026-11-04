#!/usr/bin/env bash
set -Eeuo pipefail

# Independent black-box oracle for the private, deployment-injected launcher.
# The public wrapper is checked only for its one-Place interface.  The fake
# backend is a separately compiled native ELF, so executable identity remains
# the same production check rather than an interpreter or same-name shortcut.
package=${1:?pair package path required}
fake=${2:?compiled native fake-backend path required}
launcher=$package/libexec/herdr-pair-launcher
core=$package/bin/herdr-nexus-daemon
translator=$package/bin/herdr-translator
public=$package/bin/herdr-pair
meta=$package/bin/herdr-nexus-meta

for executable in "$launcher" "$core" "$translator" "$public" "$meta" "$fake"; do
  [[ -x $executable ]]
done
readelf -h "$fake" | grep -q 'Type:'

base=$(mktemp -d "${TMPDIR:-/tmp}/b.XXXXXX")
declare -a launchers=()
current_case=preflight
case_index=0
failure() {
  local status=$? line=${BASH_LINENO[0]:-unknown}
  trap - ERR
  set +e
  echo "BACKEND_PAIR_CASE_FAILED case=$current_case line=$line status=$status" >&2
  echo "COMMAND=$launcher PLACE=${place:-<unset>} DEPLOYMENT=${deployment:-<unset>} CORE=$core TRANSLATOR=$translator 70000 8388608 25 1048576 64" >&2
  if [[ -n ${pair_pid:-} && -r /proc/$pair_pid/status ]]; then
    echo "PAIR_PID=$pair_pid CHILDREN=$(cat /proc/$pair_pid/task/$pair_pid/children 2>/dev/null)" >&2
    sed -n '/^Name:/p;/^State:/p;/^Pid:/p;/^PPid:/p;/^Uid:/p' "/proc/$pair_pid/status" >&2
    for child in $(cat "/proc/$pair_pid/task/$pair_pid/children" 2>/dev/null); do
      echo "CHILD_PID=$child EXE=$(readlink "/proc/$child/exe" 2>/dev/null)" >&2
      sed -n '/^Name:/p;/^State:/p;/^Pid:/p;/^PPid:/p;/^Uid:/p' "/proc/$child/status" >&2
    done
  fi
  for log in "${case_root:-}/pair.log" "${case_root:-}/loser.log"; do
    [[ -f $log ]] && { echo "--- $log" >&2; cat "$log" >&2; }
  done
  if [[ -n ${state_root:-} && -f $state_root/fixture.path-failure ]]; then
    echo "--- $state_root/fixture.path-failure" >&2
    cat "$state_root/fixture.path-failure" >&2
  fi
  for root in "${runtime:-}" "${config_root:-}" "${state_root:-}"; do
    [[ -e $root ]] && find "$root" -xdev -printf 'RESOURCE dev=%D ino=%i mode=%m type=%y path=%p\n' >&2
  done
  exit "$status"
}
trap failure ERR
cleanup() {
  trap - ERR
  set +e
  for pid in "${launchers[@]:-}"; do
    if [[ -n $pid ]]; then kill "$pid" 2>/dev/null || true; fi
  done
  for pid in "${launchers[@]:-}"; do
    if [[ -n $pid ]]; then wait "$pid" 2>/dev/null || true; fi
  done
  find "$base" -xdev -print >"$base.deletion-list"
  awk -v root="$base" 'index($0, root) != 1 { exit 1 }' "$base.deletion-list" || return
  rm -rf -- "$base"
  rm -f -- "$base.deletion-list"
}
trap cleanup EXIT

case_root= place= deployment= config_root= state_root= runtime= state= pair_pid=
prepare() {
  current_case=$1
  case_index=$((case_index + 1))
  local name=m$case_index
  case_root=$base/$case_index
  runtime=$case_root/run/metaflow/$name
  state=$case_root/state
  config_root=$state/c
  state_root=$state/s
  mkdir -p "$runtime" "$state" "$case_root/home"
  place="{ $name $runtime $state }"
  deployment="{ $fake [ c ] [ s ] o 3000 200 400 20 1048576 }"
}

launch() {
  local log=$case_root/pair.log
  HOME=$case_root/home \
  HERDR_CONFIG_PATH=/poison/config \
  HERDR_SOCKET_PATH=/poison/api.sock \
  HERDR_CLIENT_SOCKET_PATH=/poison/client.sock \
  HERDR_SESSION=poison \
    "$launcher" "$place" "$deployment" "$core" "$translator" \
      70000 8388608 25 1048576 64 >"$log" 2>&1 &
  pair_pid=$!
  launchers+=("$pair_pid")
}

wait_exit() {
  local pid=$1
  for _ in $(seq 1 300); do
    [[ ! -r /proc/$pid/stat ]] && return 0
    [[ $(cut -d' ' -f3 "/proc/$pid/stat") == Z ]] && return 0
    sleep .02
  done
  echo "process $pid did not exit" >&2
  return 1
}

wait_backend() {
  local session; session=$(session_name)
  for _ in $(seq 1 300); do
    [[ -f $state_root/fixture.backend-environment && \
       -S $config_root/herdr/sessions/$session/herdr.sock && \
       -S $config_root/herdr/sessions/$session/herdr-client.sock ]] && return 0
    kill -0 "$pair_pid" 2>/dev/null || break
    sleep .02
  done
  cat "$case_root/pair.log" >&2
  return 1
}

session_name() { printf '%s' "${place#\{ }" | cut -d' ' -f1; }

require_backend_ready() {
  local session; session=$(session_name)
  # The pair's backend gate performs the JSON status call through retained
  # directory handles; its sockets and later Nexus readiness are the witness.
  wait_backend
  [[ -S $config_root/herdr/sessions/$session/herdr.sock ]]
  [[ -S $config_root/herdr/sessions/$session/herdr-client.sock ]]
}

wait_nexus() {
  for _ in $(seq 1 300); do
    [[ -S $runtime/herdr-meta.sock && -S $runtime/herdr-meta-translator.sock ]] && return 0
    kill -0 "$pair_pid" 2>/dev/null || break
    sleep .02
  done
  cat "$case_root/pair.log" >&2
  return 1
}

configure_ready() {
  local name output=
  name=$(session_name)
  wait_nexus
  XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" "Configure.HerdrPath.$fake" >/dev/null
  XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" 'Configure.CallTimeout.200' >/dev/null
  XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" 'Configure.OutputLimit.1048576' >/dev/null
  XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" 'Configure.TextBudget.{ 1048576 64 }' >/dev/null
  XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" 'Configure.StartTimeout.1000' >/dev/null
  XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" 'Configure.Polling.100' >/dev/null
  XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" 'Configure.FrameCapacity.8388608' >/dev/null
  XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" 'Configure.SubscriberCapacity.8' >/dev/null
  output=$(XDG_RUNTIME_DIR=$case_root/run "$meta" "$name" 'Configure.TransportTimeout.200')
  [[ $output == *Ready* ]]
}

assert_environment_file() {
  local record=$1
  grep -Eq '^XDG_CONFIG_HOME=/proc/self/fd/[0-9]+$' "$record"
  grep -Eq '^XDG_STATE_HOME=/proc/self/fd/[0-9]+$' "$record"
  grep -Fx "XDG_CONFIG_HOME_TARGET=$config_root" "$record"
  grep -Fx "XDG_STATE_HOME_TARGET=$state_root" "$record"
  grep -Fx "HOME=$case_root/home" "$record"
  grep -Fx 'HERDR_CONFIG_PATH=<unset>' "$record"
  grep -Fx 'HERDR_SOCKET_PATH=<unset>' "$record"
  grep -Fx 'HERDR_CLIENT_SOCKET_PATH=<unset>' "$record"
  grep -Fx 'HERDR_SESSION=<unset>' "$record"
}

stop_pair() {
  local pid=$pair_pid
  kill "$pid"
  wait_exit "$pid"
  wait "$pid" 2>/dev/null || true
}

assert_no_children() {
  local pid
  for pid in "$@"; do
    ! kill -0 "$pid" 2>/dev/null
  done
}

assert_admission_closed() {
  local socket
  for socket in herdr.sock herdr-meta.sock herdr-translator.sock herdr-meta-translator.sock; do
    [[ ! -S $runtime/$socket ]]
  done
}

# Public surface remains exactly one Place; extra deployment data is private.
[[ $("$public" 2>&1 || printf ':%s' "$?") == *':64' ]]
[[ $("$public" '{}' extra 2>&1 || printf ':%s' "$?") == *':64' ]]

# Positive owned lifecycle, typed Ready, exact environment and State survival.
prepare positive
touch "$state/unrelated-state"
mkdir -p "$config_root/herdr/sessions/unrelated"
touch "$config_root/herdr/sessions/unrelated/keep"
launch
require_backend_ready
configure_ready positive
assert_environment_file "$state_root/fixture.backend-environment"
workspace=$case_root/workspace
mkdir -p "$workspace"
positive_name=$(session_name)
XDG_RUNTIME_DIR=$case_root/run "$package/bin/herdr-nexus" "$positive_name" \
  "Create.{ $positive_name $workspace [] }" | grep -q 'Created.Created'
assert_environment_file "$state_root/fixture.client-environment"
read -r -a positive_children <<<"$(cat /proc/$pair_pid/task/$pair_pid/children)"
[[ ${#positive_children[@]} -eq 3 ]]
stop_pair
assert_no_children "${positive_children[@]}"
assert_admission_closed
[[ -f $state/unrelated-state ]]
[[ -f $config_root/herdr/sessions/unrelated/keep ]]
[[ ! -e $config_root/herdr/sessions/$positive_name/herdr.sock ]]
[[ ! -e $config_root/herdr/sessions/$positive_name/herdr-client.sock ]]

# Concurrent claimant loses without disturbing the proven owner.
prepare concurrent
launch
require_backend_ready
winner=$pair_pid
HOME=$case_root/home "$launcher" "$place" "$deployment" "$core" "$translator" \
  70000 8388608 25 1048576 64 >"$case_root/loser.log" 2>&1 &
loser=$!; launchers+=("$loser")
wait_exit "$loser"; ! wait "$loser"
kill -0 "$winner"
require_backend_ready
pair_pid=$winner; stop_pair

# Pre-existing claim and backend sockets are conflicts; neither is adopted.
prepare stale-claim
touch "$runtime/o"
launch; wait_exit "$pair_pid"; ! wait "$pair_pid"
[[ -f $runtime/o ]]

prepare stale-socket
stale_name=$(session_name)
mkdir -p "$config_root/herdr/sessions/$stale_name"
touch "$config_root/herdr/sessions/$stale_name/herdr.sock"
launch; wait_exit "$pair_pid"; ! wait "$pair_pid"
[[ -f $config_root/herdr/sessions/$stale_name/herdr.sock ]]

# Descriptor-confined roots reject a symlink component when it is used.
prepare symlink-root
mkdir -p "$case_root/outside"
ln -s "$case_root/outside" "$state/c"
launch; wait_exit "$pair_pid"; ! wait "$pair_pid"
[[ -z $(find "$case_root/outside" -mindepth 1 -print -quit) ]]

# Invalid configuration is rejected before a backend effect.
for invalid in \
  "{ $fake [] [ s ] o 3000 200 400 20 1048576 }" \
  "{ $fake [ c ] [] o 3000 200 400 20 1048576 }" \
  "{ $fake [ .. ] [ s ] o 3000 200 400 20 1048576 }" \
  "{ $fake [ backend/config ] [ s ] o 3000 200 400 20 1048576 }" \
  "{ $fake [ c ] [ s ] ../claim 3000 200 400 20 1048576 }" \
  "{ relative-backend [ c ] [ s ] o 3000 200 400 20 1048576 }" \
  "{ $fake [ c ] [ s ] o 0 200 400 20 1048576 }" \
  "{ $fake [ c ] [ s ] o 3000 0 400 20 1048576 }" \
  "{ $fake [ c ] [ s ] o 3000 200 0 20 1048576 }" \
  "{ $fake [ c ] [ s ] o 3000 200 400 0 1048576 }" \
  "{ $fake [ c ] [ s ] o 3000 200 400 20 0 }"; do
  prepare invalid-$RANDOM
  deployment=$invalid
  launch; wait_exit "$pair_pid"; ! wait "$pair_pid"
  [[ ! -f $state/s/fixture.backend-environment ]]
done

# Backend startup failure is bounded and creates no pair sockets.
prepare partial
mkdir -p "$state_root"; touch "$state_root/fail-before-ready"
launch; wait_exit "$pair_pid"; ! wait "$pair_pid"
assert_admission_closed

# Readiness rejects each exit-zero false report.
for mode in status-not-running status-malformed status-incompatible status-wrong-socket status-wrong-session; do
  prepare ready-$mode
  mkdir -p "$state_root"; touch "$state_root/$mode"
  launch; wait_exit "$pair_pid"; ! wait "$pair_pid"
  assert_admission_closed
done

# A mismatched configured Herdr executable is a typed refusal, not adoption.
prepare mismatch
launch; require_backend_ready; wait_nexus
mismatch_reply=$(XDG_RUNTIME_DIR=$case_root/run "$meta" "$(session_name)" 'Configure.HerdrPath./bin/false')
[[ $mismatch_reply == *ConfigureRefused* ]]
kill -0 "$pair_pid"
stop_pair

# Loss of backend, core, or translator closes admission and tears siblings down.
for role in backend core translator; do
  prepare loss-$role
  launch; require_backend_ready; configure_ready loss-$role
  read -r -a children <<<"$(cat /proc/$pair_pid/task/$pair_pid/children)"
  [[ ${#children[@]} -eq 3 ]]
  for child in "${children[@]}"; do kill -0 "$child"; done
  target=
  for child in "${children[@]}"; do
    executable=$(readlink "/proc/$child/exe")
    case $role:$executable in
      backend:"$fake") target=$child ;;
      core:*herdr-nexus-daemon) target=$child ;;
      translator:*herdr-translator) target=$child ;;
    esac
  done
  [[ -n $target ]]
  kill "$target"
  wait_exit "$pair_pid"; ! wait "$pair_pid"
  for child in "${children[@]}"; do wait_exit "$child"; done
  assert_admission_closed
done

# The graceful path and bounded escalation both reap the exact owned backend.
prepare graceful
launch; require_backend_ready
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
stop_pair
wait_exit "$backend_pid"
assert_admission_closed

prepare escalation
mkdir -p "$state_root"; touch "$state_root/ignore-term"
launch; require_backend_ready
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
stop_pair
wait_exit "$backend_pid"
assert_admission_closed
[[ ! -e $runtime/o ]]

echo BACKEND_PAIR_INDEPENDENT_OK
