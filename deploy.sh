#/bin/sh

REGION=$1
ENV_FILE=$2

echo "Deploying lambda to region $REGION"

cargo lambda deploy test-rust \
    --binary-name test-rust \
    --enable-function-url \
    --region $REGION \
    --iam-role "arn:aws:iam::506625725958:role/Radical-Lambda" \
    --env-file $ENV_FILE
