#/bin/sh

REGION=$1
ENV_STR=$2
FUNC_NAME=$3
BLOB_FILE=$4


echo "Deploying $FUNC_NAME to region $REGION with blob file $BLOB_FILE"

# Using a binary name allows us to use the same binary for multiple functions
# This is important since each Radical function is a single binary wrapped around a wasm blob
# Need a url to talk to the function
# Include the env file that tells the function where to find the checker and remote
# Include the serialized function in the deployment, with a hard coded name
# We could also pass the binary name as an argument to the function but this is more straightforward

cargo lambda deploy "$FUNC_NAME" \
    --binary-name test-rust \
    --enable-function-url \
    --region "$REGION" \
    --iam-role "arn:aws:iam::506625725958:role/Radical-Lambda" \
    --env-vars "$ENV_STR" \
    --include "function.serialized:$BLOB_FILE" \
    --memory 2048 \
    --timeout 600
