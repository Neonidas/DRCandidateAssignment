from google.cloud import bigquery

'''
Currently unused. A separate table for companies can be used in case of duplicate companies.
'''
schema = [
    bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("catchPhrase", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("bs", "STRING", mode="NULLABLE")
]