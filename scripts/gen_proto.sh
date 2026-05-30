#!/usr/bin/env bash
# Regenerate the gRPC/protobuf stubs under src/sepp/_pb from the vendored proto.
#
# The proto contract is vendored under proto/queue.proto. It is NOT the source
# of truth — see the header comment in that file for provenance and how to
# refresh the snapshot. The vendored copy has the canonical `buf.validate`
# annotations stripped, so codegen depends only on the well-known types and
# produces a self-contained module (no buf/validate dependency).
#
# The generated stubs are committed so installing the package needs no protoc.
# protoc emits a flat `import queue_pb2`; we rewrite it to live under
# `sepp._pb` so the package imports cleanly regardless of sys.path.
set -euo pipefail

cd "$(dirname "$0")/.."

PY="${PYTHON:-.venv/bin/python}"
OUT="src/sepp/_pb"

rm -rf "$OUT"
mkdir -p "$OUT"

"$PY" -m grpc_tools.protoc \
  -I proto \
  --python_out="$OUT" \
  --grpc_python_out="$OUT" \
  --pyi_out="$OUT" \
  proto/queue.proto

# Rewrite protoc's flat import to a package-absolute one under sepp._pb.
perl -i -pe 's{^import queue_pb2 as}{from sepp._pb import queue_pb2 as}' \
  "$OUT/queue_pb2_grpc.py"

# Mark the generated directory as a package.
cat > "$OUT/__init__.py" <<'EOF'
# Generated protobuf/gRPC stubs for the sepp wire contract.
#
# Do not edit by hand — regenerate with scripts/gen_proto.sh. See that script
# and proto/queue.proto for how the vendored proto is refreshed.
EOF

echo "Generated stubs in $OUT"
