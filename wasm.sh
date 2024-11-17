#!/bin/sh

echo "Building wasm..."
(cd ../rust-wasm/wasm-func/; ./build.sh)
echo "Moving wasm binary"
cp ../rust-wasm/wasm-func/output.wasm ./function.wasm