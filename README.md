# Introduction

test-rust is a Rust project that implements an AWS Lambda function in Rust.

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install)
- [Cargo Lambda](https://www.cargo-lambda.info/guide/installation.html)

## Building

To build the project for production, run `cargo lambda build --release`. Remove the `--release` flag to build for development.

Read more about building your lambda function in [the Cargo Lambda documentation](https://www.cargo-lambda.info/commands/build.html).

## Testing

You can run regular Rust unit tests with `cargo test`.

If you want to run integration tests locally, you can use the `cargo lambda watch` and `cargo lambda invoke` commands to do it.

First, run `cargo lambda watch` to start a local server. When you make changes to the code, the server will automatically restart.

Second, you'll need a way to pass the event data to the lambda function.

You can use the existent [event payloads](https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/lambda-events/src/fixtures) in the Rust Runtime repository if your lambda function is using one of the supported event types.

You can use those examples directly with the `--data-example` flag, where the value is the name of the file in the [lambda-events](https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/lambda-events/src/fixtures) repository without the `example_` prefix and the `.json` extension.

```bash
cargo lambda invoke --data-example apigw-request
```

For generic events, where you define the event data structure, you can create a JSON file with the data you want to test with. For example:

```json
{
    "command": "test"
}
```

Then, run `cargo lambda invoke --data-file ./data.json` to invoke the function with the data in `data.json`.

For HTTP events, you can also call the function directly with cURL or any other HTTP client. For example:

```bash
curl https://localhost:9000
```

Read more about running the local server in [the Cargo Lambda documentation for the `watch` command](https://www.cargo-lambda.info/commands/watch.html).
Read more about invoking the function in [the Cargo Lambda documentation for the `invoke` command](https://www.cargo-lambda.info/commands/invoke.html).

## Deploying

To deploy the project, run `cargo lambda deploy`. This will create an IAM role and a Lambda function in your AWS account.

Read more about deploying your lambda function in [the Cargo Lambda documentation](https://www.cargo-lambda.info/commands/deploy.html).

## Running Radical

Cargo lambda works as normal, invoke using `cargo lambda invoke test-rust --data-file test-event.json`

To modify that, I wrote a little script `update-test-body.sh` which takes as input `test-event.json` and a second json file, which contains formatted arguments to a function. For example, for a function that just reads three keys, it might look like:

```
{
	"args": {
		"items": [
			"synth-item-0",
			"synth-item-1"
		],
		"delay": 0,
		"tablename": "radical_testing"
	}
}
```
`main.rs` will pass those arguments to the wasm function (hopefully correctly). So, you create a json file with the arguments to your function, and then run `update-test-body.sh` with your json file as the argument.

i.e. `./update-test-body.sh synth-reader.json`, which should produce a file you can pass to `cargo lambda invoke` for local testing

In totality, you need three things:

1) A web assembly function pair (actual function and key guesser), these should be in the same `.wasm` file, and that should be compiled using the local serializer to give you a `function.serialized` file, which you copy to this directory.
2) The wrapper (this directory). Run using `cargo lambda watch`, and invoke using above instructions
3) A json file with the properly formatted arguments for your web assembly function. The example above matches for one of the synthetic functions. Make sure to use `update-test-body.sh` to get that into a properly escaped JSON string that's embedded in some other json file that's formatted for AWS Lambda to understand. You should then be able to invoke your function
