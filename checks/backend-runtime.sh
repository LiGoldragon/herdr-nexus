#!/usr/bin/env bash
set -Eeuo pipefail

backend=${1:?backend executable}
base=$(mktemp -d /tmp/hbr.XXXXXX)
server=
cleanup() {
  set +e
  [[ -n $server ]] && kill "$server" 2>/dev/null
  [[ -n $server ]] && wait "$server" 2>/dev/null
  find "$base" -xdev -print >"$base.deletion-list"
  awk -v root="$base" 'index($0, root) != 1 { exit 1 }' "$base.deletion-list" || return
  rm -rf -- "$base" "$base.deletion-list"
}
trap cleanup EXIT

export HOME=$base/home
export XDG_CONFIG_HOME=$base/config
export XDG_STATE_HOME=$base/state
mkdir -p "$HOME" "$XDG_CONFIG_HOME" "$XDG_STATE_HOME"
touch "$XDG_STATE_HOME/unrelated"
session=runtime
api=$XDG_CONFIG_HOME/herdr/sessions/$session/herdr.sock
client=$XDG_CONFIG_HOME/herdr/sessions/$session/herdr-client.sock

"$backend" --session "$session" server >"$base/server.out" 2>"$base/server.err" &
server=$!
for _ in $(seq 1 300); do
  [[ -S $api && -S $client ]] && break
  kill -0 "$server" 2>/dev/null || { cat "$base/server.err" >&2; exit 1; }
  sleep .02
done
[[ -S $api && -S $client ]]
kill -0 "$server"

status=$("$backend" --session "$session" status server --json)
[[ $status == *'"status":"running"'* ]]
[[ $status == *'"running":true'* ]]
[[ $status == *'"compatible":true'* ]]
[[ $status == *"\"socket\":\"$api\""* ]]
[[ $status == *"\"session\":\"$session\""* ]]

if "$backend" --session "$session" server >"$base/conflict.out" 2>"$base/conflict.err"; then
  echo 'a second backend unexpectedly acquired the live session' >&2
  exit 1
fi
kill -0 "$server"

"$backend" --session "$session" server stop
for _ in $(seq 1 300); do
  kill -0 "$server" 2>/dev/null || break
  sleep .02
done
! kill -0 "$server" 2>/dev/null
wait "$server"
server=
[[ ! -e $api && ! -e $client ]]
[[ -f $XDG_STATE_HOME/unrelated ]]
echo BACKEND_RUNTIME_OK
