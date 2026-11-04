# A committed contract is exactly the Rust emitted by the pinned Ethos Zero.
set -eu

temporary="$(mktemp -d)"
cleanup() {
  rm -rf "$temporary"
}
trap cleanup EXIT

name="$(basename "$declaration" .ethos)"
status=0
reply="$("$generator/bin/ethos-zero" "Generate.{ «$declaration» «$temporary» }" 2>&1)" || status=$?
case "$status:$reply" in
  0:Generated.*) ;;
  *) echo "$declaration (exit $status): $reply" >&2; exit 1 ;;
esac
cmp "$temporary/$name.rs" "$committed"
touch "$out"
