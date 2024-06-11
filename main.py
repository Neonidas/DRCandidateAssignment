import sys
import requests

import UserSchema
import config

from google.cloud import bigquery
from google.oauth2 import service_account


def fetch_json(url, limit=None):
    response = requests.get(url)
    JSON = response.json()[:limit]
    return JSON


def authenticate_bigquery_client_with_keyfile(path_to_keyfile, location=None) -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(path_to_keyfile)
    # insert Error handling here
    return bigquery.Client(credentials=credentials, project=credentials.project_id, location=location)


def dataset_in_project(client, dataset_id):
    datasets = list(client.list_datasets())
    return dataset_id in [dataset.dataset_id for dataset in datasets]


def main():
    key_path = sys.argv[1:2]
    if not key_path:
        print("Error: Please provide path to keyfile as command line argument")
        return

    client = authenticate_bigquery_client_with_keyfile(key_path[0], location=config.location)

    if not dataset_in_project(client, config.dataset_id):
        print("Error: dataset could not be found in project")
        return

    table_id = f"{config.project_id}.{config.dataset_id}.behlul_users"
    if "behlul_users" in [table.table_id for table in list(client.list_tables(dataset=config.dataset_id))]:
        print("Table already exists")
    else:
        table = bigquery.Table(table_id, schema=UserSchema.schema)
        table_id = client.create_table(table)

    users = fetch_json()
    errors = client.insert_rows_json(table_id, users)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print("New rows have been added.")

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = client.query(
        (
            "SELECT "
            "*"
            "FROM"
            "`bigquery-public-data.github_repos.commits`"
        ),
        job_config=job_config,
        location="US"
    )
    print(f"This query will process {query_job.total_bytes_processed} bytes")
    print(f"This query will be billed for {query_job.total_bytes_billed} bytes")
    print("The query will be billed for 0 DKK according to Google Cloud Pricing Calculator:")
    print("https://cloud.google.com/products/calculator?hl=da&dl=CiRkNmE5NzEwZi03ZDhiLTRmNzctOTVjNS03YjViYmMzM2EzOGQQCxokMTEwRjQwQzMtMTQwMC00RTYxLUJGMDYtREYzQzQ2NERDRkZC")


if __name__ == "__main__":
    main()
