
### data.py
- In `get_data` function extracted the data from the API in json format.
- Created a Spark Session and then created a rdd of the data from extracted from API. 
- In `create_and_clean_df` function cleaned the data, created a schema and finally created a dataframe from the schema and rdd.
- In `csv_file` function user can download the results of each query in csv file.
- For finding `most affected state`, sorted the Death/Total_cases in descending order and limit the result by 1 and selected the state column from the result.
- For finding `least affected state`, sorted the Death/Total_cases in ascending order and limit the result by 1 and selected the state column from the result.
- For finding `state with most cases`, sorted the total_cases column in descending order and limit the result by 1 and selected the state column from the result.
- For finding `state with least cases`, sorted the total_cases column in ascending order and limit the result by 1 and selected the state column from the result.
- For finding the `total cases across all states`, aggregated the sum of total_cases column.
- For finding `most cured state`, sorted the Cured/Total_Cases in descending order and limit the result by 1 and selected the state column from the result.
- For finding `least affected state`, sorted the Cured/Total_Cases in ascending order and limit the result by 1 and selected the state column from the result.
- At last stored the result of each query in a python list.


### app.py
In app.py created a `Flask app` and inside it created the routes for all the queries used in data.py and retured the output in json format.
