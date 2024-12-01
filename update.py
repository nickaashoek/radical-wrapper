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

def setup_envs(check_url, data_url):
    '''
    Setup two strings that serve as the environment variables for the two functions
    User must have: CHECK_URL, REMOTE_URL, and DEPLOYMENT
    Data must have: DEPLOYMENT only, but fill in both to be safe
    '''
    user_env = f"CHECK_URL={check_url},REMOTE_URL={data_url},DEPLOYMENT=edge"
    data_env = f"CHECK_URL={check_url},REMOTE_URL={data_url},DEPLOYMENT=datacenter"
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
    parser.add_argument("--build", dest="build", action="store_true", default=False, help="build the function as well")
    args = parser.parse_args()
    
    if args.build:
        command = subprocess.run(["cargo", "lambda", "build", "--release", "--arm64"])
    
    user_client = boto3.client('lambda', region_name=args.user_region)
    data_client = boto3.client('lambda', region_name=args.data_region)

    exists = func_exists(user_client, args.func_name) and func_exists(data_client, args.func_name)
    if not exists:
        print(f"Function {args.func_name} not found in one of {args.user_region} or {args.data_region}. Deploy first, then fill in.")
        # We must deploy first to figure out the proper URLs
        # So just upload with "blank" envs (actual blank envs throw an error)
        deploy_function(args.user_region, "TEST_ENV=dummy", args.func_name, args.wasm_blob)
        deploy_function(args.data_region, "TEST_ENV=dummy", args.func_name, args.wasm_blob)
        exists = True
    else:
        print(f"Function {args.func_name} found in {args.user_region}. Checking URLs and then maybe updating.")
    
    data_url = fetch_url(data_client, args.data_region, args.func_name)
    check_url = get_check_url(args.data_region)
    print("Located near data function at:", data_url)
    print("Located check server at:", check_url)
    user_env, data_env = setup_envs(check_url, data_url)

    deploy_function(args.user_region, user_env, args.func_name, args.wasm_blob)
    print("Done deploying user function")
    deploy_function(args.data_region, data_env, args.func_name, args.wasm_blob)
    print("Done deploying data function")