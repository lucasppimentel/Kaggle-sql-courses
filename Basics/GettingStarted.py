from google.cloud import bigquery

client = bigquery.Client()

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the "hacker_news" dataset
tables = list(client.list_tables(dataset))

for table in tables:
    print(table.id)

table_ref = dataset_ref.table("full")

table = client.get_table(table_ref)

print(table.schema)

client.list_rows(table, selected_fields=table.schema[:1], max_results=5).to_dataframe()