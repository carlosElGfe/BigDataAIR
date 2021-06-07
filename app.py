import os
import time
import csv
from utils import *
from flask import Flask, render_template
from python_jobs.newjob import *
from python_jobs.listings_pre import *
import pyathenajdbc
import boto3
app = Flask(__name__)

@app.route("/")
def index():
    data = get_reviews()
    return render_template("home.html", message="Hello Flask!",data2 = data)

@app.route("/hosts")
def hosts():
    data,worsts = get_hosts_data()
    return render_template("hosts.html", message="Hello Flask!",data2 = data,data=worsts)

@app.route("/listings")
def listingss():
    get_listings()
    return ("ok")

@app.route("/test")
def test():
    aws_access_key_id="AKIA4UBERADC3AQYKUCB"
    aws_secret_access_key="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"
    athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    response = athena.start_query_execution(
                QueryString = 'SELECT reviewer_name,sum(score)as score FROM "grupo5_db"."reviews_amsterdam" group by reviewer_name order by score desc limit 10',
                    QueryExecutionContext={
                    'Database': 'grupo5_db'
                    },
                    ResultConfiguration={
                    'OutputLocation': 's3://aypmd-grupo5/query_results',
                    }
            )
    query_execution_id = response['QueryExecutionId']
    time.sleep(10)
    responsee = athena.get_query_results(QueryExecutionId = query_execution_id)
    output = []
    cont = 0
    for i in responsee['ResultSet']['Rows']:
        if cont != 0:
            data = i['Data']
            temp = []
            for j in data:
                temp.append(j['VarCharValue'])
            output.append(temp)
        else:
            cont+=1
    print(output)        
    #print(responsee['ResultSet']['Rows'])
    print(type(responsee))
    return render_template("hosts.html", message="Hello Flask!",data2 = [],data=output)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)