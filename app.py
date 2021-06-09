import os
import time
import csv
from utils import *
from flask import Flask, render_template, request
from python_jobs.listings_pre import *
import boto3
from werkzeug.datastructures import ImmutableMultiDict
import sys

countries = [
    'Sydney',
    'Estambul',
    'Paris',
    'Amsterdam'
]
app = Flask(__name__)
key = sys.argv[1]
secret = sys.argv[2]
@app.route("/")
def index():
    data = get_reviews()
    return render_template("home.html", message="Hello Flask!",data2 = data)



@app.route("/listings")
def listingss():
    get_listings()
    return ("ok")

@app.route('/bar')
def bar():
    bar_labels=labels
    bar_values=values
    print(labels)
    return render_template('bar_chart.html', title='Bitcoin Monthly Price in USD', max=17000, labels=bar_labels, values=bar_values)


@app.route("/hosts")
def test():
    aws_access_key_id=key
    aws_secret_access_key=secret
    athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    data = []
    for i in countries:
        data.append(get_worst_and_bests_hosts(i.lower(),athena))
    return render_template("hosts.html", message="Hello Flask!",data = data,max=2000,countries = countries)

@app.route("/ai_info")
def ai_info():
    return render_template("info.html")

@app.route("/db")
def db():
    return render_template("database.html",query='',lenn = 0)

@app.route("/query", methods = ['POST'])
def query():
    #print(request.form)
    dictt = request.form.to_dict(flat=False)
    for i in dictt:
        try:
            aws_access_key_id=key
            aws_secret_access_key=secret
            athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            temp = exequte_query(dictt[i][0],athena)
        except Exception:
            temp = ''
    return render_template("database.html",query=temp,lenn = len(temp))

@app.route("/neighborhoods")
def barrios():
    c = countries
    c = c[:3]
    aws_access_key_id=key
    aws_secret_access_key=secret
    athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    data = []
    for i in c:
        temp = get_score_neighborhood(i.lower(),athena)
        data.append(temp)  
    return render_template('barrios.html',  max=17000,countries = countries,data = data)

@app.route("/beds")
def beds():
    c = countries
    c = c[:3]
    aws_access_key_id=key
    aws_secret_access_key=secret
    athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    data = []
    for i in c:
        temp = get_score_beds(i.lower(),athena)
        data.append(temp)  
    return render_template('beds.html',  max=17000,countries = countries,data = data)




if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)