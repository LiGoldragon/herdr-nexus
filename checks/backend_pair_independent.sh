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
declare -A active_launchers=()
current_case=preflight
case_index=0
failure() {
  local status=$? line=${BASH_LINENO[0]:-unknown}
  trap - ERR
  set +e
  echo "BACKEND_PAIR_CASE_FAILED case=$current_case line=$line status=$status" >&2
  echo "COMMAND=$launcher PLACE=${place:-<unset>} DEPLOYMENT=${deployment:-<unset>} CORE=${launch_core:-$core} TRANSLATOR=${launch_translator:-$translator} 70000 8388608 25 1048576 64" >&2
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
  for pid in "${!active_launchers[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${!active_launchers[@]}"; do
    wait "$pid" 2>/dev/null || true
    unset 'active_launchers[$pid]'
  done
  find "$base" -xdev -print >"$base.deletion-list"
  awk -v root="$base" 'index($0, root) != 1 { exit 1 }' "$base.deletion-list" || return
  rm -rf -- "$base"
  rm -f -- "$base.deletion-list"
}
trap cleanup EXIT

case_root= place= deployment= config_root= state_root= runtime= state= pair_pid=
expected_unknown=
expected_backend_closed=1
last_stop_elapsed=
launch_core=$core launch_translator=$translator
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
  launch_core=$core
  launch_translator=$translator
  expected_unknown=
  expected_backend_closed=1
}

launch() {
  local log=$case_root/pair.log
  HOME=$case_root/home \
  HERDR_CONFIG_PATH=/poison/config \
  HERDR_SOCKET_PATH=/poison/api.sock \
  HERDR_CLIENT_SOCKET_PATH=/poison/client.sock \
  HERDR_SESSION=poison \
  FIXTURE_RUNTIME=$runtime \
    "$launcher" "$place" "$deployment" "$launch_core" "$launch_translator" \
      70000 8388608 25 1048576 64 >"$log" 2>&1 &
  pair_pid=$!
  active_launchers[$pair_pid]=1
}

wait_fixture_child() {
  local expected_executable=${1:-$fake} pid=
  for _ in $(seq 1 300); do
    pid=$(sed -n 's/^FIXTURE_BLOCKING_CHILD pid=//p' "$case_root/pair.log" | tail -n 1)
    if [[ -n $pid && -r /proc/$pid/stat && $(readlink "/proc/$pid/exe") == "$expected_executable" ]]; then
      printf '%s' "$pid"
      return 0
    fi
    kill -0 "$pair_pid" 2>/dev/null || break
    sleep .02
  done
  cat "$case_root/pair.log" >&2
  return 1
}

reap_launcher() {
  local pid=$1 status
  set +e
  wait "$pid"
  status=$?
  set -e
  unset 'active_launchers[$pid]'
  return "$status"
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

process_starttime() {
  local pid=$1 record
  record=$(cat "/proc/$pid/stat" 2>/dev/null) || return 1
  awk '{ print $22 }' <<<"$record"
}

wait_exact_exit() {
  local pid=$1 expected_starttime=$2 record state actual_starttime
  for _ in $(seq 1 300); do
    if ! record=$(cat "/proc/$pid/stat" 2>/dev/null); then
      echo "EXACT_PROCESS_REAPED pid=$pid starttime=$expected_starttime"
      return 0
    fi
    state=$(awk '{ print $3 }' <<<"$record")
    actual_starttime=$(awk '{ print $22 }' <<<"$record")
    if [[ $actual_starttime != "$expected_starttime" ]]; then
      echo "EXACT_PROCESS_IDENTITY_REUSED pid=$pid prior_starttime=$expected_starttime current_starttime=$actual_starttime"
      return 0
    fi
    sleep .02
  done
  echo "exact process $pid starttime=$expected_starttime did not exit" >&2
  sed -n '/^Name:/p;/^State:/p;/^Pid:/p;/^PPid:/p;/^Uid:/p' "/proc/$pid/status" >&2 || true
  echo "EXE=$(readlink "/proc/$pid/exe" 2>/dev/null) STAT=$(cat "/proc/$pid/stat" 2>/dev/null)" >&2
  return 1
}

stop_startup_phase() {
  local children=("$@") child starts=() index began elapsed pair_status unknown_identity=
  [[ ${#children[@]} -ge 1 ]]
  for child in "${children[@]}"; do
    [[ $(awk '{ print $4 }' "/proc/$child/stat") == "$pair_pid" ]]
    starts+=("$(process_starttime "$child")")
  done
  touch "$runtime/unrelated" "$state/unrelated-state"
  if [[ -n $expected_unknown ]]; then
    unknown_identity=$(stat -Lc '%d:%i' "$expected_unknown")
  fi
  began=$(date +%s%3N)
  kill "$pair_pid"
  wait_exit "$pair_pid"
  set +e
  wait "$pair_pid"
  pair_status=$?
  set -e
  unset 'active_launchers[$pair_pid]'
  elapsed=$(($(date +%s%3N) - began))
  last_stop_elapsed=$elapsed
  [[ $pair_status == 0 ]]
  # StopTimeout is 400 ms in Deployment. Scheduler allowance is outside the
  # launcher's budget assertion; an additive per-child timeout would exceed
  # this bound by several multiples.
  (( elapsed < 650 ))
  for ((index = 0; index < ${#children[@]}; index++)); do
    wait_exact_exit "${children[$index]}" "${starts[$index]}"
  done
  if [[ -f $runtime/fixture.stop-parent-children ]]; then
    # children[0] is the backend being asked to stop. Every paired child must
    # already have been reaped before the orderly backend command is spawned.
    for ((index = 1; index < ${#children[@]}; index++)); do
      ! grep -qw "${children[$index]}" "$runtime/fixture.stop-parent-children"
    done
  fi
  assert_admission_closed_except "$expected_unknown"
  if (( expected_backend_closed )); then
    assert_backend_closed
  else
    local session; session=$(session_name)
    [[ -S $config_root/herdr/sessions/$session/herdr.sock ]]
    [[ -S $config_root/herdr/sessions/$session/herdr-client.sock ]]
  fi
  [[ ! -e $runtime/o ]]
  [[ -f $runtime/unrelated ]]
  [[ -f $state/unrelated-state ]]
  if [[ -n $expected_unknown ]]; then
    [[ -S $expected_unknown ]]
    [[ $(stat -Lc '%d:%i' "$expected_unknown") == "$unknown_identity" ]]
    echo "PRESERVED_UNKNOWN_SOCKET path=$expected_unknown identity=$unknown_identity"
  fi
}

event_time() {
  local event=$1
  awk -v event="$event" '$3 == event { print $1; exit }' "$runtime/fixture.lifecycle"
}

assert_event_before() {
  local first=$1 second=$2 first_time second_time
  first_time=$(event_time "$first")
  second_time=$(event_time "$second")
  [[ -n $first_time && -n $second_time ]]
  (( first_time <= second_time ))
}

assert_event_delta() {
  local first=$1 second=$2 minimum=$3 maximum=$4 first_time second_time delta
  first_time=$(event_time "$first")
  second_time=$(event_time "$second")
  [[ -n $first_time && -n $second_time ]]
  delta=$((second_time - first_time))
  (( delta >= minimum && delta <= maximum ))
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
    [[ -S $runtime/herdr-nexus-meta.sock && -S $runtime/herdr-nexus-meta-translator.sock ]] && return 0
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
  reap_launcher "$pid"
}

assert_no_children() {
  local pid
  for pid in "$@"; do
    ! kill -0 "$pid" 2>/dev/null
  done
}

assert_admission_closed() {
  assert_admission_closed_except ""
}

assert_admission_closed_except() {
  local exception=$1
  local socket
  for socket in herdr-nexus.sock herdr-nexus-meta.sock herdr-nexus-translator.sock herdr-nexus-meta-translator.sock; do
    [[ $runtime/$socket == "$exception" ]] && continue
    [[ ! -S $runtime/$socket ]]
  done
}

assert_backend_closed() {
  local session; session=$(session_name)
  [[ ! -S $config_root/herdr/sessions/$session/herdr.sock ]]
  [[ ! -S $config_root/herdr/sessions/$session/herdr-client.sock ]]
}

# Public surface remains exactly one Place; extra deployment data is private.
[[ $("$public" 2>&1 || printf ':%s' "$?") == *':64' ]]
[[ $("$public" '{}' extra 2>&1 || printf ':%s' "$?") == *':64' ]]

# A stop is consumed, and every exact owned child is reaped, at each
# externally distinguishable startup phase. Checkpoints before the first
# effect and exactly between the three initial ACKs and the steady loop have
# no black-box witness and are covered by source-private units.
prepare stop-backend-readiness
mkdir -p "$state_root"; touch "$state_root/status-not-running"
launch; wait_backend
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
[[ -n $(process_starttime "$backend_pid") ]]
stop_startup_phase "$backend_pid"

prepare stop-core-publication
launch_core=$fake
launch; require_backend_ready
core_fixture_pid=$(wait_fixture_child)
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
[[ $core_fixture_pid != "$backend_pid" ]]
stop_startup_phase "$backend_pid" "$core_fixture_pid"
assert_event_before PAIRED_CHILD_TERM_OBSERVED BACKEND_STOP_REQUESTED

prepare stop-translator-publication
launch_translator=$fake
launch; require_backend_ready
translator_fixture_pid=$(wait_fixture_child)
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
[[ $translator_fixture_pid != "$backend_pid" ]]
core_pid=
for child in $(cat /proc/$pair_pid/task/$pair_pid/children); do
  executable=$(readlink "/proc/$child/exe" 2>/dev/null)
  [[ $executable == *herdr-nexus-daemon ]] && core_pid=$child
done
[[ -n $core_pid ]]
stop_startup_phase "$backend_pid" "$core_pid" "$translator_fixture_pid"
assert_event_before PAIRED_CHILD_TERM_OBSERVED BACKEND_STOP_REQUESTED

# Forced teardown at each paired-child startup edge remains within the same
# single StopTimeout. The backend stop is attempted only after the child that
# ignored TERM has been killed and reaped.
for forced_role in core translator; do
  prepare forced-$forced_role-publication
  uncooperative_child=$case_root/uncooperative-child-$forced_role
  cp "$fake" "$uncooperative_child"
  chmod +x "$uncooperative_child"
  if [[ $forced_role == core ]]; then
    launch_core=$uncooperative_child
  else
    launch_translator=$uncooperative_child
  fi
  launch; require_backend_ready
  forced_pid=$(wait_fixture_child "$uncooperative_child")
  backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
  forced_children=("$backend_pid")
  if [[ $forced_role == translator ]]; then
    core_pid=
    for child in $(cat /proc/$pair_pid/task/$pair_pid/children); do
      executable=$(readlink "/proc/$child/exe" 2>/dev/null)
      [[ $executable == *herdr-nexus-daemon ]] && core_pid=$child
    done
    [[ -n $core_pid ]]
    forced_children+=("$core_pid")
  fi
  forced_children+=("$forced_pid")
  forced_starttime=$(process_starttime "$forced_pid")
  backend_starttime=$(process_starttime "$backend_pid")
  stop_startup_phase "${forced_children[@]}"
  wait_exact_exit "$forced_pid" "$forced_starttime"
  wait_exact_exit "$backend_pid" "$backend_starttime"
  grep -q ' PAIRED_CHILD_TERM_IGNORED$' "$runtime/fixture.lifecycle"
  grep -q ' BACKEND_STOP_REQUESTED$' "$runtime/fixture.lifecycle"
  assert_event_delta PAIRED_CHILD_TERM_IGNORED BACKEND_STOP_REQUESTED 50 250
  (( last_stop_elapsed >= 150 ))
done

# Even when an earlier paired child needed forced reaping and the backend also
# ignores orderly stop and TERM, global expiry cancels and reaps both exact
# process lifetimes. No backend-exit record is expected after SIGKILL.
prepare forced-global-expiry
uncooperative_child=$case_root/uncooperative-child-core
cp "$fake" "$uncooperative_child"
chmod +x "$uncooperative_child"
launch_core=$uncooperative_child
mkdir -p "$state_root"; touch "$state_root/ignore-term"
expected_backend_closed=0
launch; require_backend_ready
forced_pid=$(wait_fixture_child "$uncooperative_child")
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
forced_starttime=$(process_starttime "$forced_pid")
backend_starttime=$(process_starttime "$backend_pid")
stop_startup_phase "$backend_pid" "$forced_pid"
wait_exact_exit "$forced_pid" "$forced_starttime"
wait_exact_exit "$backend_pid" "$backend_starttime"
grep -q ' PAIRED_CHILD_TERM_IGNORED$' "$runtime/fixture.lifecycle"
grep -q ' BACKEND_TERM_OBSERVED$' "$runtime/fixture.lifecycle"
! grep -q ' BACKEND_EXITING$' "$runtime/fixture.lifecycle"
assert_event_delta PAIRED_CHILD_TERM_IGNORED BACKEND_STOP_REQUESTED 50 250
(( last_stop_elapsed >= 300 ))

# An uncooperative child that never publishes leaves a socket whose identity
# the parent never received. Closing its private channel and reaping it must
# not turn that unknown pathname into parent cleanup authority.
prepare stop-unpublished-unknown
unknown_translator=$case_root/uncooperative-translator
cp "$fake" "$unknown_translator"
chmod +x "$unknown_translator"
launch_translator=$unknown_translator
expected_unknown=$runtime/herdr-nexus-translator.sock
launch; require_backend_ready
for _ in $(seq 1 300); do
  unknown_pid=$(sed -n 's/^FIXTURE_UNKNOWN_CHILD pid=\([0-9][0-9]*\) path=.*/\1/p' "$case_root/pair.log" | tail -n 1)
  [[ -n $unknown_pid && -S $expected_unknown ]] && break
  kill -0 "$pair_pid" 2>/dev/null || break
  sleep .02
done
[[ -n ${unknown_pid:-} && -S $expected_unknown ]]
[[ $(readlink "/proc/$unknown_pid/exe") == "$unknown_translator" ]]
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
core_pid=
for child in $(cat /proc/$pair_pid/task/$pair_pid/children); do
  executable=$(readlink "/proc/$child/exe" 2>/dev/null)
  [[ $executable == *herdr-nexus-daemon ]] && core_pid=$child
done
[[ -n $core_pid ]]
stop_startup_phase "$backend_pid" "$core_pid" "$unknown_pid"

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
backend_pid=$(cat "$state_root/fixture.$positive_name.pid")
positive_starts=()
for child in "${positive_children[@]}"; do
  positive_starts+=("$(process_starttime "$child")")
done
stop_pair
for ((index = 0; index < ${#positive_children[@]}; index++)); do
  wait_exact_exit "${positive_children[$index]}" "${positive_starts[$index]}"
done
grep -q ' BACKEND_STOP_REQUESTED$' "$runtime/fixture.lifecycle"
for child in "${positive_children[@]}"; do
  [[ $child == "$backend_pid" ]] && continue
  ! grep -qw "$child" "$runtime/fixture.stop-parent-children"
done
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
loser=$!; active_launchers[$loser]=1
wait_exit "$loser"; ! reap_launcher "$loser"
kill -0 "$winner"
require_backend_ready
pair_pid=$winner; stop_pair

# Pre-existing claim and backend sockets are conflicts; neither is adopted.
prepare stale-claim
touch "$runtime/o"
launch; wait_exit "$pair_pid"; ! reap_launcher "$pair_pid"
[[ -f $runtime/o ]]

prepare stale-socket
stale_name=$(session_name)
mkdir -p "$config_root/herdr/sessions/$stale_name"
touch "$config_root/herdr/sessions/$stale_name/herdr.sock"
launch; wait_exit "$pair_pid"; ! reap_launcher "$pair_pid"
[[ -f $config_root/herdr/sessions/$stale_name/herdr.sock ]]

# Descriptor-confined roots reject a symlink component when it is used.
prepare symlink-root
mkdir -p "$case_root/outside"
ln -s "$case_root/outside" "$state/c"
launch; wait_exit "$pair_pid"; ! reap_launcher "$pair_pid"
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
  launch; wait_exit "$pair_pid"; ! reap_launcher "$pair_pid"
  [[ ! -f $state/s/fixture.backend-environment ]]
done

# Backend startup failure is bounded and creates no pair sockets.
prepare partial
mkdir -p "$state_root"; touch "$state_root/fail-before-ready"
launch; wait_exit "$pair_pid"; ! reap_launcher "$pair_pid"
assert_admission_closed

# Readiness rejects each exit-zero false report.
for mode in status-not-running status-malformed status-incompatible status-wrong-socket status-wrong-session; do
  prepare ready-$mode
  mkdir -p "$state_root"; touch "$state_root/$mode"
  launch; wait_exit "$pair_pid"; ! reap_launcher "$pair_pid"
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
  child_starts=()
  for child in "${children[@]}"; do
    child_starts+=("$(process_starttime "$child")")
  done
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
  wait_exit "$pair_pid"; ! reap_launcher "$pair_pid"
  for ((index = 0; index < ${#children[@]}; index++)); do
    wait_exact_exit "${children[$index]}" "${child_starts[$index]}"
  done
  assert_admission_closed
done

# The graceful path and bounded escalation both reap the exact owned backend.
prepare graceful
launch; require_backend_ready
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
backend_starttime=$(process_starttime "$backend_pid")
stop_pair
wait_exact_exit "$backend_pid" "$backend_starttime"
assert_admission_closed

prepare escalation
mkdir -p "$state_root"; touch "$state_root/ignore-term"
launch; require_backend_ready
backend_pid=$(cat "$state_root/fixture.$(session_name).pid")
read -r -a escalation_children <<<"$(cat /proc/$pair_pid/task/$pair_pid/children)"
[[ ${#escalation_children[@]} -eq 3 ]]
escalation_starts=()
for child in "${escalation_children[@]}"; do
  escalation_starts+=("$(process_starttime "$child")")
done
escalation_began=$(date +%s%3N)
stop_pair
escalation_elapsed=$(($(date +%s%3N) - escalation_began))
(( escalation_elapsed < 650 ))
for ((index = 0; index < ${#escalation_children[@]}; index++)); do
  wait_exact_exit "${escalation_children[$index]}" "${escalation_starts[$index]}"
  [[ ${escalation_children[$index]} == "$backend_pid" ]] && continue
  ! grep -qw "${escalation_children[$index]}" "$runtime/fixture.stop-parent-children"
done
grep -q ' BACKEND_STOP_REQUESTED$' "$runtime/fixture.lifecycle"
grep -q ' BACKEND_TERM_OBSERVED$' "$runtime/fixture.lifecycle"
! grep -q ' BACKEND_EXITING$' "$runtime/fixture.lifecycle"
assert_admission_closed
[[ ! -e $runtime/o ]]

echo BACKEND_PAIR_INDEPENDENT_OK
