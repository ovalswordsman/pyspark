## main.py

1. Built a spark session
```
spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()
```

2.
```
def get_data():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": "8e26c15ff2mshe0e03bd340a6194p10fa52jsnfb48dc150447",
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)
    data = response.json()
    data.popitem()
    data.popitem()
    return data

```

Using get_data() function, fetching the api data into response variable. Cleaning the data using popitem() function

3.
```
def create_csv(data):
    with open('output.csv', mode='w', newline='') as file:
        
        # create a CSV writer
        writer = csv.writer(file)
        
        # write the header row
        writer.writerow(['slno', 'state', 'confirm','cured','death','total'])
        
        # write the data rows
        for state_data in data.values():
            writer.writerow([state_data['slno'], state_data['state'], state_data['confirm'],state_data['cured'],state_data['death'],state_data['total']])
```
Using create_csv() function, created a csv file from the incoming data from the api


## api.py

```
def createdataframefromcsv():
    df = spark.read.format("csv").option("header","true").load("output.csv")#convert the csv to a data frame to run queryies on
    df.printSchema()
    df.show()
    return df
```

using this function created the dataframe from the output file created to use in the functions later


```
@app.route('/')#the home route displays a menu of all the information to gain
def home():
     return jsonify({'/show_csv_data' : " SHOW CSV FILE DATA",
                     '/show_api_data' : " SHOW API RETURNED DATA",
                     '/mk_csv_df':" MAKE A CSV FILE FROM THE DATA RETURNED FROM API",
                     '/get_most_affected':" MOST AFFECTED STATE( total death/total covid cases)",
                     '/get_least_affected':" LEAST AFFECTED STATE ( total death/total covid cases)",
                     '/highest_covid_cases':" STATE WITH THE HIGHEST COVID CASES",
                     '/least_covid_cases':" STATE WITH THE LEAST COVID CASES",
                     '/total_cases':" TOTAL CASES",
                     '/handle_well':" STATE THAT HANDLED THE MOST COVID CASES EFFICIENTLY( total recovery/ total covid cases)",
                     '/least_well':" STATE THAT HANDLED THE MOST COVID CASES LEAST EFFICIENTLY( total recovery/ total covid cases)",
                     '/least_suffering' : "State least suffering from covid ( least critical cases)",
                     '/still_suffering' : "State still suffering from covid (highest critical cases)."
                     })
```
the home route displays a menu of all the information to gain!
<img width="978" alt="Screenshot 2023-04-30 at 10 16 11 PM" src="https://user-images.githubusercontent.com/54627996/235365428-6127c6fb-0868-444f-aaf4-d63205740553.png">

```
@app.route('/mk_csv_df')
def make_csv():
    json_object=get_data()#api data with last two records gone
    create_csv(json_object)#creating the csv file from the dictionary returned
    return  "<h1>:FILE CREATED</h1>"
```
Using this function creating the output.csv file

<img width="756" alt="Screenshot 2023-04-30 at 10 24 43 PM" src="https://user-images.githubusercontent.com/54627996/235365860-956c5bf8-15af-45cb-98b3-cad64faca70b.png">

```
@app.route('/show_api_data')    
def show_api_data(): #Function to directly fetch data from the api to display to user
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": "8e26c15ff2mshe0e03bd340a6194p10fa52jsnfb48dc150447",
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)
    csv_columns = ['slno','state','confirm','cured','death','total']
    data =response.json()
    return data
```

Using this function, show the exact data that is coming from the api

<img width="736" alt="Screenshot 2023-04-30 at 10 29 06 PM" src="https://user-images.githubusercontent.com/54627996/235366101-2ef8c6b0-9071-4d95-b831-94a85be3aa2c.png">


```
@app.route('/get_most_affected')
def get_most_affected_state():
    if not os.path.exists("output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()#create the data fram of the csv file to work on
        df.createOrReplaceTempView("TABLE")#get a temporary view of the dataframe to write sql queries on
        ans=spark.sql("SELECT state, death/confirm AS affected FROM table").orderBy("affected",ascending=False).select("state").limit(1).collect()
        state=ans[0][0]#select the state and use the formula in the question and order in descending and only return the top value to the variable
        return jsonify({'Most affected state ': state}) 

```
Getting the most affected state 

<img width="501" alt="Screenshot 2023-04-30 at 10 39 35 PM" src="https://user-images.githubusercontent.com/54627996/235366558-0201521e-589b-448c-8198-8829341f2134.png">

```
@app.route('/get_least_affected')
def get_least_affected_state():
     if not os.path.exists("output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
     else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")
        sub=spark.sql("SELECT state, death/confirm AS affected FROM TABLE").orderBy("affected",ascending=True).select("state").limit(1).collect()
        state=sub[0][0]#the same thing as above but ordered in ascending order
        return jsonify({'Least affected state ': state})


@app.route('/highest_covid_cases')
def highest_covid_cases():
    if not os.path.exists("output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")
        sub=df.orderBy("confirm",ascending=False).select("state","confirm").limit(1).collect()
        state=sub[0][0] #order in descending by the confirmed covid cases and then return the top record to the variable
        confirmed=sub[0][1]
        return jsonify({'State with highest cases ': state,'Confirmed Covid cases':confirmed})


@app.route('/least_covid_cases')
def least_covid_cases():
    if not os.path.exists("output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")
        sub=df.orderBy("confirm",ascending=True).select("state","confirm").limit(1).collect()
        state=sub[0][0]#order in ascending by the confirmed covid cases and then return the top record to the variable
        confirmed=sub[0][1]
        return jsonify({'State with the least cases':state,'Confirmed Covid cases':confirmed})


@app.route('/total_cases')
def total_cases():
    if not os.path.exists("output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")
        sub=spark.sql("SELECT SUM(total) AS TOTALCASES FROM TABLE").collect()#SUM function to sum all the numbers under the totalcases field and return that
        return jsonify({'Total cases':sub[0][0]})

@app.route('/handle_well')
def state_handle_well():
    if not os.path.exists("output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")#USE THE FORMULA GIVEN IN QUESTION AND USE EFFICIANCY TO ORDER BY DESCENDING ORDER AND RETURN THE TOP MOST RECORD
        sub=spark.sql("SELECT STATE , CURED/CONFIRM AS efficiancy from table").orderBy("efficiancy",ascending=False).limit(1).collect()
        return jsonify({'WELL HANDLED STATE':sub[0][0],'Efficiancy':sub[0][1]})

@app.route('/least_well')
def state_least_well():
    if not os.path.exists("output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")#USE THE FORMULA GIVEN IN QUESTION AND USE EFFICIANCY TO ORDER BY ASCENDING ORDER AND RETURN THE TOP MOST RECORD
        sub=spark.sql("SELECT STATE , CURED/CONFIRM AS efficiancy from table").orderBy("efficiancy",ascending=True).select("state","efficiancy").limit(1).collect()
        return jsonify({'LEAST WELL HANDLED STATE':sub[0][0],'Efficiancy':sub[0][1]})
```
