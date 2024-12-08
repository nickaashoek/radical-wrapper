#!/bin/sh

DATAREGION=$1
USERREGION=$2

echo "Updating in data region $DATAREGION and user region $USERREGION"

declare -a funcs=("retwis-follow" "retwis-login" "retwis-post" "retwis-timeline" "retwis-profile")

for f in "${funcs[@]}"
do
    echo "Updating $f"
    python update.py --user-region $USERREGION --data-region $DATAREGION --wasm-blob "../radical-serializer/functions/$f.serialized" --function $f
done
