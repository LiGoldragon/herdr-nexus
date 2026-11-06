#!/usr/bin/env bash
# Both entry points prepare their place before anything is spawned, bound or opened: the
# launcher (herdr-pair) before its directory checks and its children, the direct daemon before
# its sockets. Cases: fresh, repeated start, legacy-only byte-preserving move, destination-only
# reopen, both stores refused with both unchanged, failure after the move with a completing retry,
# two daemons started together on one legacy place, and no socket or child after a refusal.
set -euo pipefail

package=${1:?package}
base=$(mktemp -d)
pair=
daemon=
second=
started=

stop_all() {
  for pid in "$pair" "$daemon" "$second"; do
    if [[ -n $pid ]]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  pair=
  daemon=
  second=
}
cleanup() {
  set +e
  stop_all
  chmod -R u+rwx "$base" 2>/dev/null
  rm -rf "$base"
}
trap cleanup EXIT
trap 'status=$?; echo "failed at line $LINENO" >&2; cat "$base"/*.log >&2 2>/dev/null; exit $status' ERR

export XDG_RUNTIME_DIR=$base/run
export HOME=$base/home
unset XDG_STATE_HOME
directory=$XDG_RUNTIME_DIR/nexus/one
state=$HOME/.local/state/nexus/one
legacy=$HOME/.local/state/metaflow/one
place="{ one $directory $state }"

tree() { (cd "$1" && find . -type f -print0 | sort -z | xargs -0 sha256sum); }
pair_bound() { [[ -S $directory/herdr-nexus-meta-translator.sock && -S $directory/herdr-nexus-meta.sock ]]; }
prepared() { [[ -d $directory && -d $state && ! -e $legacy ]]; }
wait_for() {
  for _ in $(seq 1 200); do
    "$1" && return 0
    sleep .02
  done
  cat "$base"/*.log >&2
  return 1
}
reset() {
  stop_all
  chmod -R u+rwx "$base/run" "$base/home" 2>/dev/null || true
  rm -rf "$base/run" "$base/home" "$base"/*.log
  mkdir -p "$base/run" "$base/home"
}
seed_legacy() {
  mkdir -p "$legacy/nested"
  printf 'first\0second\n' >"$legacy/record"
  printf '\377\000\376' >"$legacy/nested/binary"
}
start_pair() { "$package/bin/herdr-pair" "$place" >"$base/pair.log" 2>&1 & pair=$!; }
start_daemon() {
  local log=$1
  "$package/bin/herdr-nexus-place" "$place" Core "$(id -u)" "$$" 70000 8388608 25 "instance-$log" \
    | "$package/bin/herdr-nexus-daemon" >"$base/$log.log" 2>&1 &
  started=$!
}
refused_pair() {
  start_pair
  for _ in $(seq 1 200); do
    kill -0 "$pair" 2>/dev/null || break
    sleep .02
  done
  if kill -0 "$pair" 2>/dev/null; then
    echo "the pair kept running after a refused preparation" >&2
    return 1
  fi
  if wait "$pair"; then
    echo "the pair exited zero after a refused preparation" >&2
    return 1
  fi
  pair=
  ! pair_bound
}
refused_daemon() {
  start_daemon daemon
daemon=$started
  for _ in $(seq 1 200); do
    kill -0 "$daemon" 2>/dev/null || break
    sleep .02
  done
  if kill -0 "$daemon" 2>/dev/null; then
    echo "the daemon kept running after a refused preparation" >&2
    return 1
  fi
  if wait "$daemon"; then
    echo "the daemon exited zero after a refused preparation" >&2
    return 1
  fi
  daemon=
  ! [[ -S $directory/herdr-nexus.sock || -S $directory/herdr-nexus-meta.sock ]]
}

# launcher: fresh, then a repeated start changes nothing
reset
start_pair
wait_for pair_bound
[[ -d $directory && -d $state && ! -e $legacy ]]
printf 'kept' >"$state/record"
before=$(tree "$state")
stop_all
start_pair
wait_for pair_bound
stop_all
[[ $before == $(tree "$state") && ! -e $legacy ]]

# launcher: legacy-only state moves byte for byte
reset
seed_legacy
before=$(tree "$legacy")
start_pair
wait_for pair_bound
stop_all
[[ ! -e $legacy && $before == $(tree "$state") ]]

# launcher: destination-only state reopens unchanged, twice
reset
mkdir -p "$state"
printf 'current' >"$state/record"
before=$(tree "$state")
start_pair
wait_for pair_bound
stop_all
start_pair
wait_for pair_bound
stop_all
[[ $before == $(tree "$state") && ! -e $legacy ]]

# launcher: both stores refused, both unchanged, nothing bound
reset
mkdir -p "$legacy" "$state"
printf 'legacy' >"$legacy/record"
printf 'current' >"$state/record"
before_legacy=$(tree "$legacy")
before_current=$(tree "$state")
refused_pair
grep -qE 'StateConflict|both exist' "$base/pair.log"
[[ $before_legacy == $(tree "$legacy") && $before_current == $(tree "$state") && ! -e $directory ]]

# launcher: failure after the move leaves the destination, the retry completes
if [[ $(id -u) != 0 ]]; then
  reset
  seed_legacy
  before=$(tree "$legacy")
  chmod 500 "$base/run"
  refused_pair
  [[ ! -e $legacy && $before == $(tree "$state") ]]
  chmod 700 "$base/run"
  start_pair
  wait_for pair_bound
  stop_all
  [[ $before == $(tree "$state") ]]
fi

# direct daemon: legacy-only state moves and the runtime is created; this case observes the
# preparation only, the daemon binds its meta socket right after and waits for its translator peer,
# and a later bind is not certified here
reset
seed_legacy
before=$(tree "$legacy")
start_daemon daemon
daemon=$started
wait_for prepared
stop_all
[[ ! -e $legacy && $before == $(tree "$state") ]]

# direct daemon: both stores refused, nothing bound, both unchanged
reset
mkdir -p "$legacy" "$state"
printf 'legacy' >"$legacy/record"
printf 'current' >"$state/record"
before_legacy=$(tree "$legacy")
before_current=$(tree "$state")
refused_daemon
grep -q 'place preparation failed' "$base/daemon.log"
[[ $before_legacy == $(tree "$legacy") && $before_current == $(tree "$state") && ! -e $directory ]]

# two daemons started together on one legacy place: preparation serializes, state moves once
reset
seed_legacy
before=$(tree "$legacy")
start_daemon first
daemon=$started
start_daemon second
second=$started
wait_for prepared
sleep .3
stop_all
[[ ! -e $legacy && $before == $(tree "$state") ]]
! grep -qE 'both exist|place preparation failed|No such file' "$base/first.log" "$base/second.log"

echo PLACE_PREPARATION_OK
