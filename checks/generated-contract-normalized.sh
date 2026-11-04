# A committed contract is the pinned Ethos Zero output plus its declared
# module-path or Contracted glue. The normalization name is Nix input.
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
generated="$temporary/$name.rs"

case "$normalization" in
  transport)
    sed -i '/^)]$/a use shared_translator::placement::Place;' "$generated"
    sed -i 's/shared_translator::Place/Place/g' "$generated"
    ;;
  public-call)
    sed -i '/^)]$/a use crate::transport_types as transport;\
use crate::public_payload;' "$generated"
    ;;
  meta-call)
    sed -i '/^)]$/a use crate::transport_types as transport;\
use crate::meta_payload;' "$generated"
    ;;
  core-meta)
    cat >> "$generated" <<'EOF'
/// The generated internal meta contract source.
pub const ETHOS: &str = include_str!("../ethos/core-meta.ethos");

impl signal::Contracted for Query {
    const CONTRACT_SOURCE: &'static str = ETHOS;
}
EOF
    ;;
  *) echo "unknown generated-contract normalization: $normalization" >&2; exit 1 ;;
esac

cmp "$generated" "$committed"
touch "$out"
