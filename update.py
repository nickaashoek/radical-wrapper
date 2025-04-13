import boto3
import argparse
import subprocess

def fetch_url(lambda_client, region, function):
    response = lambda_client.list_functions()
    functions = set([f['FunctionName'] for f in response.get('Functions', [])])
    if function not in functions:
        print(f"Function {function} not found in {region}")
        return None
    func_data = lambda_client.get_function(FunctionName=function)
    func_arn = func_data['Configuration']['FunctionArn']
    url = lambda_client.get_function_url_config(FunctionName=func_arn)
    return url['FunctionUrl']

def get_check_url(region):
    ec2_client = boto3.client('ec2', region_name=region)
    instances = ec2_client.describe_instances()
    for r in instances['Reservations']:
        for instance in r['Instances']:
            for tag in instance['Tags']:
                if tag['Key'] == 'Name' and tag['Value'] == 'CheckServer':
                    return "http://{}:8000".format(instance['PublicIpAddress'])

def setup_envs(check_url, data_url, use_scylla):
    '''
    Setup two strings that serve as the environment variables for the two functions
    User must have: CHECK_URL, REMOTE_URL, and DEPLOYMENT
    Data must have: DEPLOYMENT only, but fill in both to be safe
    '''
    user_env = f"CHECK_URL={check_url},REMOTE_URL={data_url},DEPLOYMENT=edge,USE_SCYLLA=false"
    if use_scylla:
        # Only the near user function can use scylla. The remote ones always use dynamo.
        user_env = f"CHECK_URL={check_url},REMOTE_URL={data_url},DEPLOYMENT=edge,USE_SCYLLA=true"
    data_env = f"CHECK_URL={check_url},REMOTE_URL={data_url},DEPLOYMENT=datacenter,USE_SCYLLA=false"
    return user_env, data_env


def deploy_function(region, env_str, func_name, blob_path):
    subprocess.run(["sh", "./deploy.sh", region, env_str, func_name, blob_path])

def func_exists(lambda_client, function):
    response = lambda_client.list_functions()
    functions = set([f['FunctionName'] for f in response.get('Functions', [])])
    return function in functions

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-region", dest="user_region", type=str, help="Near user region where function exists")
    parser.add_argument("--data-region", dest="data_region", type=str, help="Near data region where function exists")
    parser.add_argument("--function", dest="func_name", type=str, help="Function name to setup", required=True)
    parser.add_argument("--wasm-blob", dest="wasm_blob", type=str, help="Path to the blob to include with the function deploy", default="function.serialized")
    parser.add_argument("--use-scylla", dest="use_scylla", action="store_true", default=False, help="Use scylla instead of dynamo. Renames the function appropriately.")
    parser.add_argument("--build", dest="build", action="store_true", default=False, help="build the function as well")
    args = parser.parse_args()

    if args.build:
        command = subprocess.run(["cargo", "lambda", "build", "--release", "--arm64"])

    user_client = boto3.client('lambda', region_name=args.user_region)
    data_client = boto3.client('lambda', region_name=args.data_region)

    data_name = args.func_name
    user_name = args.func_name + "_syclla" if args.use_scylla else args.func_name

    user_exists = func_exists(user_client, user_name)
    data_exists = func_exists(data_client, data_name)

    if not user_exist:
        print(f"Function {user_name} not found in {args.user_region}. Deploy first, then fill in.")
        deploy_function(args.user_region, "TEST_ENV=dummy", user_name, args.wasm_blob)
    else:
        print(f"Function {user_name} found in {args.user_region}. Checking URLs and then maybe updating.")
    if not data_exists:
        print(f"Function {data_name} not found in {args.data_region}. Deploy first, then fill in.")
        deploy_function(args.data_region, "TEST_ENV=dummy", data_name, args.wasm_blob)
    else:
        print(f"Function {data_name} found in {args.data_region}. Checking URLs and then maybe updating.")

    data_url = fetch_url(data_client, args.data_region, args.func_name)
    check_url = get_check_url(args.data_region)
    print("Located near data function at:", data_url)
    print("Located check server at:", check_url)
    user_env, data_env = setup_envs(check_url, data_url, args.use_scylla)

    deploy_function(args.user_region, user_env, user_name, args.wasm_blob)
    print("Done deploying user function")
    deploy_function(args.data_region, data_env, data_name, args.wasm_blob)
    print("Done deploying data function")
