#!/bin/sh

set -e

if [ -z "$1" ] ; then
    echo "usage: $0 BLD" >&2
    exit 1
fi

BLD="$1"

grpc_in="src/determined/api/v1/api.proto"
build_path="build/proto/github.com/determined-ai/determined/proto/pkg"

rm -rf build/proto pkg
mkdir -p build/proto
# Protobuf generation.
for source in $(cat $BLD/protofiles) ; do
    protoc -I src "$source" --go_out=plugins=grpc:build/proto
done
# GRPC generation.
protoc -I src "$grpc_in" --grpc-gateway_out=logtostderr=true:build/proto
mv "$build_path" pkg
touch "$BLD/proto.stamp"
