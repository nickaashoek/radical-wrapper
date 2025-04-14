import boto3
import csv
import subprocess
import json
import argparse

def parse_profile(profile_file):
    with open(profile_file, 'r') as f:
        reader = csv.DictReader(f)
        return [row for row in reader]

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

def setup_envs(check_url, data_url, use_scylla, scylla_ep, log_level):
    '''
    Setup two strings that serve as the environment variables for the two functions
    User must have: CHECK_URL, REMOTE_URL, and DEPLOYMENT
    Data must have: DEPLOYMENT only, but fill in both to be safe
    '''
    scylla_str = "true" if use_scylla else "false"

    user_env = f"CHECK_URL={check_url},\
            REMOTE_URL={data_url},\
            DEPLOYMENT=edge,\
            USE_SCYLLA={scylla_str},\
            SCYLLA_EP={scylla_ep},\
            AWS_LAMBDA_LOG_LEVEL={log_level}"

    data_env = f"CHECK_URL={check_url},\
            REMOTE_URL={data_url},\
            DEPLOYMENT=datacenter,\
            USE_SCYLLA=false,\
            SCYLLA_EP={scylla_ep},\
            AWS_LAMBDA_LOG_LEVEL={log_level}"

    return user_env, data_env


def deploy_function(region, env_str, func_name, blob_path):
    subprocess.run(["sh", "./deploy.sh", region, env_str, func_name, blob_path])

def func_exists(lambda_client, function):
    response = lambda_client.list_functions()
    functions = set([f['FunctionName'] for f in response.get('Functions', [])])
    return function in functions

def func_setup(lambda_client, function, blob_path):
    if not func_exists(lambda_client, function):
        print(f"Function {function} does not exist in {lambda_client.meta.region_name}. Creating...")
        deploy_function(lambda_client.meta.region_name, "TEST_ENV=dummy", function, blob_path)
        # After deploying the function, we need to give it the permission for public url? I guess?
        try:
            lambda_client.add_permission(
                FunctionName=function,
                StatementId="PublicURLAccess",
                Action="lambda:InvokeFunctionUrl",
                FunctionUrlAuthType="NONE",
                Principal="*",
            )
        except lambda_client.exceptions.ResourceConflictException:
            print(f"Permission already exists for {function}.")
        except Exception as e:
            print(f"Error adding permission to {function}: {e}")
    else:
        print(f"Function {function} already exists in {lambda_client.meta.region_name}. Updating...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", dest="profile", type=str, help="profile file for a given experimental deployment")
    parser.add_argument("--functions", dest="functions", type=str, help="JSON file listing all the functions to deploy")
    parser.add_argument("--data-region", dest="data_region", nargs="+", type=str, help="which region to use for the backup function", default=["us-east-2"])
    parser.add_argument("--use-scylla", dest="use_scylla", action="store_true", default=False, help="Whether to deploy using scylla or dynamo")
    parser.add_argument("--build", dest="build", action="store_true", default=False, help="build the wrapper as well")
    parser.add_argument("--log-level", dest="log_level", type=str, default="INFO", help="Set the logging level (DEBUG, INFO, WARNING, ERROR)")

    args = parser.parse_args()

    # Parse the deployment profile
    data_regions = []
    user_regions = []
    with open(args.profile, 'r') as f:
        dictreader = csv.DictReader(f)
        for row in csv.DictReader(f):
            if row['region'] in args.data_region:
                data_regions.append(row)
            else:
                user_regions.append(row)

    # Parse the function profile
    with open(args.functions, 'r') as f:
        functions = json.load(f)

    if args.use_scylla:
        for function in functions.keys():
            functions[function+"_scylla"] = functions[function]
            del functions[function]

    for function in functions:
        print("Function to deploy:", function, "with blob:", functions[function])

    # Deploy the functions to reach region and configure them properly
    backup_urls = dict()
    # Setup the data functions first
    check_url = f"http://{data_regions[0]['check_server']}:8000"

    for region in data_regions:
        data_client = boto3.client('lambda', region_name=region['region'])
        region_name = region['region']
        scylla_ep = f"http://{region['syclla_ip']}:8192"
        for function, blob_path in functions.items():
            func_setup(data_client, function, blob_path)
            backup_url = fetch_url(data_client, region_name, function)
            backup_urls[function] = backup_url
            _, data_env = setup_envs(check_url, backup_url, args.use_scylla, scylla_ep, args.log_level)
            deploy_function(region_name, data_env, function, blob_path)

    for region in user_regions:
        user_client = boto3.client('lambda', region_name=region['region'])
        # Setup the user functions
        for function, blob_path in functions.items():
            func_setup(user_client, function, blob_path)
            # Get the appropriate bacup url
            scylla_ep = f"http://{region['syclla_ip']}:8192"
            backup_url = backup_urls[function]
            user_env, _ = setup_envs(check_url, backup_url, args.use_scylla, scylla_ep, args.log_level)
            deploy_function(region['region'], user_env, function, blob_path)

