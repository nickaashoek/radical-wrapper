#/bin/sh

REGION=$1

echo "Deploying lambda to region $REGION"

cargo lambda deploy test-rust \
    --enable-function-url \
    --region $REGION \
    --iam-role "arn:aws:iam::506625725958:role/Radical-Lambda" \
    --env-file .env.default
