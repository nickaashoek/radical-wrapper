import boto3
import argparse
import dotenv
import subprocess

def fetch_url(region, function):
    client = boto3.client('lambda', region_name=region)
    response = client.list_functions()
    functions = set([f['FunctionName'] for f in response.get('Functions', [])])
    if function not in functions:
        print(f"Function {function} not found in {region}")
        return None
    func_data = client.get_function(FunctionName=function)
    func_arn = func_data['Configuration']['FunctionArn']
    url = client.get_function_url_config(FunctionName=func_arn)
    return url['FunctionUrl']
 
def get_check_url(region):
    client = boto3.client('ec2', region_name=region)
    instances = client.describe_instances()
    for r in instances['Reservations']:
        for instance in r['Instances']:
            for tag in instance['Tags']:
                if tag['Key'] == 'Name' and tag['Value'] == 'CheckServer':
                    return "http://{}:8000".format(instance['PublicIpAddress'])

def update_urls(user_env, check_url, data_url):
    user_env_file = dotenv.find_dotenv(user_env)
    dotenv.set_key(user_env_file, "CHECK_URL", check_url)
    dotenv.set_key(user_env_file, "REMOTE_URL", data_url)
    

def deploy_function(region, env_path):
    command = subprocess.run(["sh", "./deploy.sh", region, env_path])
    print(command.stdout)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-region", dest="user_region", type=str, help="Near user region where function exists")
    parser.add_argument("--data-region", dest="data_region", type=str, help="Near data region where function exists")
    parser.add_argument("--function", dest="func_name", type=str, help="Function name to setup")
    parser.add_argument("--user-env", dest="user_env", default=".env.user", type=str, help="User environment file")
    parser.add_argument("--data-env", dest="data_env", default=".env.data", type=str, help="Data environment file")
    parser.add_argument("--deploy", dest="deploy", action="store_true", default=False, help="just deploy the function function")
    parser.add_argument("--build", dest="build", action="store_true", default=False, help="build the function as well")
    args = parser.parse_args()
    
    if args.build:
        command = subprocess.run(["cargo", "lambda", "build", "--release", "--arm64"])
    
    if not args.deploy:
        data_url = fetch_url(args.data_region, args.func_name)
        check_url = get_check_url(args.data_region)
        print(data_url, check_url)
        
        update_urls(args.user_env, check_url, data_url)

    deploy_function(args.user_region, args.user_env)
    deploy_function(args.data_region, args.data_env)