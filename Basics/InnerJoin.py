from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Get a list of available tables 
table_objects = list(client.list_tables(dataset))
#list_of_tables = []
#for table in table_objects:
#    list_of_tables.append(table.table_id)

list_of_tables = [object_.table_id for object_ in table_objects]# Your code here

# Print your answer
print(list_of_tables)


# Your code here
questions_query = """
                  SELECT id, title, owner_user_id
                  FROM `bigquery-public-data.stackoverflow.posts_questions`
                  WHERE tags LIKE '%bigquery%'
                  """

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
questions_query_job = client.query(questions_query, job_config=safe_config) # Your code goes here

# API request - run the query, and return a pandas DataFrame
questions_results = questions_query_job.to_dataframe() # Your code goes here

# Preview results
print(questions_results.head())

# Your code here
answers_query = """
                  SELECT pa.id, pa.body, pa.owner_user_id
                  FROM `bigquery-public-data.stackoverflow.posts_questions` AS pq
                  INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS pa
                      ON pq.id=pa.parent_id
                  WHERE pq.tags LIKE '%bigquery%'
                  """

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=27*10**10)
answers_query_job = client.query(answers_query, job_config=safe_config) # Your code goes here

# API request - run the query, and return a pandas DataFrame
answers_results = answers_query_job.to_dataframe() # Your code goes here

# Preview results
print(answers_results.head())

# Your code here
bigquery_experts_query = """WITH user_answers AS
                          (
                              SELECT pa.id, pa.body, pa.owner_user_id
                              FROM `bigquery-public-data.stackoverflow.posts_questions` AS pq
                              INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS pa
                                  ON pq.id=pa.parent_id
                              WHERE pq.tags LIKE '%bigquery%'
                          )
                          SELECT owner_user_id AS user_id, COUNT(id) AS number_of_answers
                          FROM user_answers
                          GROUP BY owner_user_id
                          """

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
bigquery_experts_query_job = client.query(bigquery_experts_query, job_config=safe_config) # Your code goes here

# API request - run the query, and return a pandas DataFrame
bigquery_experts_results = bigquery_experts_query_job.to_dataframe() # Your code goes here

# Preview results
print(bigquery_experts_results.head())
