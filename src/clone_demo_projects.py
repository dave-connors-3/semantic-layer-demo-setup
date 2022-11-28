import json
import os
from dbtc import dbtCloudClient


client = dbtCloudClient()

teammates = [
    "Angelica Lastra",
    "Bennie Regenold",
    "Benoit Perigaud",
    "Carol Ohms",
    "Carolyn Ghosh",
    "Christine Berger",
    "Dave Connors",
    "Deanna Minnick",
    "Grace Goheen",
    "Ilan Gendellman",
    "Jess Williams",
    "Lauren Benezra",
    "Nicholas Yager",
    "Pat Kearns",
    "Rebecca Murray",
    "Sam Harting",
    "Wasila Quader"
]

account_id = 51798
snowflake_prod_user_pw = os.environ["SNOWFLAKE_PROD_USER_PW"]
api_key = os.environ["DBT_CLOUD_API_KEY"]
service_token = os.environ["DBT_CLOUD_SERVICE_TOKEN"]
clone_from_project_id = 182955


def get_sample_project():
    """
    Returns the full sample project
    """
    return client.cloud.get_project(account_id=account_id, project_id=clone_from_project_id)

def get_sample_project_env(clone_from_project_id=clone_from_project_id):
    """
    Returns the sample deployment env to be used as the template to create other envs and credentials
    """
    envs = client.cloud.list_environments(account_id, project_id=clone_from_project_id)
    deployment_env = [env for env in envs["data"] if env["type"] == "deployment"] 
    # there is only one deployment env in this clone project, otherwise not great design here
    return deployment_env[0]   

def get_sample_project_info_payload(teammate):
    """
    generate the templated branch names, schema name, and project names
    """
    schema_branch = f"sl_demo_{teammate.lower().split(' ')[0]}"
    return {
        "project_name": f"SL Demo - { teammate }",
        "branch_name": schema_branch,
        "schema_name": schema_branch,
        "connection_name": f"SL Snowflake - { teammate }",
        "env_name": f"SL Demo Env - { teammate }",
        "job_name": f"SL Demo Job - { teammate }"
    }

def create_semantic_layer_demo_project(teammate_data):
    payload = {
        "id": None,
        "name": teammate_data["project_name"],
        "dbt_project_subdirectory": None,
        "account_id": account_id,
        "connection_id": None,
        "repository_id": None
    }
    return client.cloud.create_project(account_id=account_id, payload=payload)

def create_semantic_layer_demo_connection(account_id, project_id, clone_creds, teammate_data):
    payload = {
        "id": None,
        "name": teammate_data["connection_name"],
        "type": "snowflake",
        "details": {
            "account": clone_creds["details"]["account"],
            "role": clone_creds["details"]["role"],
            "database": clone_creds["details"]["database"],
            "warehouse": clone_creds["details"]["warehouse"],
            "oauth_client_id": None,
            "oauth_client_secret": None,
            "client_session_keep_alive": False,
            "allow_sso": False,
        },
        "state": 1,
        "account_id": account_id,
        "project_id": project_id
    }
    return client.cloud.create_connection(account_id, project_id, payload)

def create_semantic_layer_demo_repository(account_id, project_id, clone_repo):
    payload = {
        "account_id": account_id,
        "project_id": project_id,
        "remote_url": clone_repo["remote_url"],
        "github_installation_id": clone_repo["github_installation_id"]
    }
    return client.cloud.create_repository(account_id, project_id, payload)

def update_semantic_layer_demo_project(project_id, repository_id, connection_id, teammate_data):
    payload = {
        "id": project_id,
        "name": teammate_data["project_name"],
        "account_id": account_id,
        "repository_id": repository_id,
        "connection_id": connection_id
    }
    return client.cloud.update_project(account_id, project_id, payload)

def create_semantic_layer_demo_credentials(project_id, teammate_data):
    payload = {
        "id": None,
        "account_id": account_id,
        "project_id": project_id,
        "state": 1,
        "threads": 4,
        "target_name": "default",
        "type": "snowflake",
        "schema": teammate_data["schema_name"],
        "auth_type": "password",
        "user": "dbt_prod",
        "password": snowflake_prod_user_pw
    }
    return client.cloud.create_credentials(account_id, project_id, payload)

def create_semantic_layer_demo_env(project_id, credentials_id, teammate_data):
    payload = {
        "id": None,
        "account_id": account_id,
        "project_id": project_id,
        "credentials_id": credentials_id,
        "name": teammate_data["env_name"],
        "dbt_version": "1.3.0-latest",
        "type": "deployment",
        "use_custom_branch": True,
        "custom_branch": teammate_data["branch_name"]
    }
    return client.cloud.create_environment(account_id, project_id, payload=payload)

def create_semantic_layer_demo_job(project_id, environment_id, teammate_data):
    payload = {
        "id": None, 
        "account_id" : account_id,
        "project_id" : project_id,
        "environment_id" : environment_id,
        "name" : teammate_data["job_name"],
        "dbt_version": None,
        "triggers" : {
            "github_webhook": False,
            "schedule": False,
            "custom_branch_only": False
        },
        "execute_steps": [
            "dbt build"
        ],
        "settings": {
            "threads": 1,
            "target_name": "default"
        },
        "state": 1,
        "generate_docs": True,
        "schedule": {
            "cron": "0 * * * 0,1,2,3,4,5,6",
            "date": {"type": "days_of_week", "days": [0, 1, 2, 3, 4, 5, 6]},
            "time": {"type": "every_hour", "interval": 1}
        }
    }
    return client.cloud.create_job(account_id, payload=payload)


def main():
    sample_project = get_sample_project()
    sample_project_connection = sample_project["data"]["connection"]
    sample_project_repository = sample_project["data"]["repository"]
    for teammate in teammates:
        teammate_data = get_sample_project_info_payload(teammate)
        create_project = create_semantic_layer_demo_project(teammate_data)
        print("Created Project:")
        print(create_project)
        project_id = create_project["data"]["id"]
        create_connection = create_semantic_layer_demo_connection(account_id, project_id, sample_project_connection, teammate_data)
        print("Created Connection:")
        print(create_connection)
        connection_id = create_connection["data"]["id"]
        create_repository = create_semantic_layer_demo_repository(account_id, project_id, sample_project_repository)
        print("Created Repository:")
        print(create_repository)
        repository_id = create_repository["data"]["id"]
        update_semantic_layer_demo_project(project_id, repository_id, connection_id, teammate_data)
        create_credentials = create_semantic_layer_demo_credentials(project_id, teammate_data)
        print("Created Credentials:")
        print(create_credentials)
        credentials_id = create_credentials["data"]["id"]
        create_env = create_semantic_layer_demo_env(project_id, credentials_id, teammate_data)
        print("Created env:")
        print(create_env)
        environment_id = create_env["data"]["id"]
        create_job = create_semantic_layer_demo_job(project_id, environment_id, teammate_data)
        print("Created job:")
        print(create_job)

if __name__ == "__main__":
    main()