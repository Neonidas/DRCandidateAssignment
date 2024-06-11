from google.cloud import bigquery

schema = [
    bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("username", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("address", "RECORD", mode="REQUIRED",
                         fields=[
                             bigquery.SchemaField("street", "STRING", mode="REQUIRED"),
                             bigquery.SchemaField("suite", "STRING", mode="REQUIRED"),
                             bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
                             bigquery.SchemaField("zipcode", "STRING", mode="REQUIRED"),
                             bigquery.SchemaField("geo", "RECORD", mode="REQUIRED",
                                                  fields=[
                                                      bigquery.SchemaField("lat", "STRING", mode="REQUIRED"),
                                                      bigquery.SchemaField("lng", "STRING", mode="REQUIRED"),
                                                  ])
                         ]),
    bigquery.SchemaField("phone", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("website", "STRING", mode="NULLABLE"),  # Allow for users with no website
    bigquery.SchemaField("company", "RECORD", mode="NULLABLE",  # Allow for users with no company
                         fields=[
                             bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                             bigquery.SchemaField("catchPhrase", "STRING", mode="NULLABLE"),
                             bigquery.SchemaField("bs", "STRING", mode="NULLABLE")
                         ])
]