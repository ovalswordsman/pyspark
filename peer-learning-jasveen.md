In app.py created a Flask app and inside it created the routes for all the queries used in data.py and retured the output in json format. <br>

### data.py
- In `load_dataset` function extracted the data from the API in json format.
- In `clean_dataset` function cleaned the data and append the desired data in a python list.
- In `create_dataframe` function created a Spark Session, then build the schema and finally created a dataframe using the schema we created.
- Created a new dataframe with `affected` as a new column which includes `Death/Total_Cases` of each state.
- For finding `most affected state`, sorted the affected column in descending order and limit the result by 1 and selected the state column from the result.
- For finding `least affected state`, sorted the affected column in ascending order and limit the result by 1 and selected the state column from the result.
- For finding `state with most cases`, sorted the total_cases column in descending order and limit the result by 1 and selected the state column from the result.
- For finding `state with least cases`, sorted the total_cases column in ascending order and limit the result by 1 and selected the state column from the result.
- For finding the `total cases across all states`, aggregated the sum of total_cases column.
- Created a new dataframe with `curred` as a new column which includes `Cured/Total_Cases` of each state.
- For finding `most cured state`, sorted the cured column in descending order and limit the result by 1 and selected the state column from the result.
- For finding `least affected state`, sorted the cured column in ascending order and limit the result by 1 and selected the state column from the result.
