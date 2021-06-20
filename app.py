import os
import time
import csv
from utils import *
from flask import Flask, render_template, request
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
key = os.environ['ACCESS_KEY']
secret = os.environ['SECRET_ACCESS_KEY']
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
    return render_template('bar_chart.html', title='BAR', max=17000, labels=bar_labels, values=bar_values)

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
            print("didnt happened")
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

@app.route("/form")
def form():
    return render_template('form.html',countries="")

@app.route("/compare",methods = ['POST'])
def compare():
    dictt = request.form.to_dict(flat=False)
    country1 = dictt['country'][0]
    country2 = dictt['country2'][0]
    aws_access_key_id=key
    aws_secret_access_key=secret
    athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    data1 = get_country_data(country1,athena)
    data2 = get_country_data(country2,athena)
    print(data1,data2)
    return render_template('form.html', data2 = data2 , data1 = data1,countries=[country1,country2])



@app.route("/view")
def view():
    c = countries
    c = c[:3]
    aws_access_key_id=key
    aws_secret_access_key=secret
    athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    data = []
    temp,temp2,temp3 = get_view(athena)
    first = []
    temp_value = list(map(lambda x: float(x[1]),temp))
    temp_label = list(map(lambda x: (x[0]),temp))    
    first.append(temp_value)
    first.append(temp_label)
    data.append(first) 
    first = []
    temp_value = list(map(lambda x: float(x[1]),temp2))
    temp_label = list(map(lambda x: (x[0]),temp2))
    first.append(temp_value)
    first.append(temp_label)
    data.append(first) 
    first = [] 
    temp_value = list(map(lambda x: float(x[1]),temp3))
    temp_label = list(map(lambda x: (x[0]),temp3))
    first.append(temp_value)
    first.append(temp_label)
    data.append(first)    
    return render_template('roomtype.html',  max=17000,countries = ['Entire Apartment','Private Suit','Shared Room'],data = data)

@app.route("/view_count")
def view_count():
    c = countries
    c = c[:3]
    aws_access_key_id=key
    aws_secret_access_key=secret
    athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    data = []
    temp,temp2,temp3 = get_view(athena)
    first = []
    temp_value = list(map(lambda x: float(x[1]),temp))
    temp_label = list(map(lambda x: (x[0]),temp))    
    first.append(temp_value)
    first.append(temp_label)
    data.append(first) 
    first = []
    temp_value = list(map(lambda x: float(x[1]),temp2))
    temp_label = list(map(lambda x: (x[0]),temp2))
    first.append(temp_value)
    first.append(temp_label)
    data.append(first) 
    first = [] 
    temp_value = list(map(lambda x: float(x[1]),temp3))
    temp_label = list(map(lambda x: (x[0]),temp3))
    first.append(temp_value)
    first.append(temp_label)
    data.append(first)    
    return render_template('roomtypecount.html',  max=17000,countries = ['Entire Apartment','Private Suit','Shared Room'],data = data)

@app.route("/review_count")
def review_count():
    c = countries
    c = c[:3]
    aws_access_key_id=key
    aws_secret_access_key=secret
    athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    data = []
    temp = get_review_count(athena)
    first = []
    temp_value = list(map(lambda x: float(x[1]),temp))
    temp_label = list(map(lambda x: (x[0]),temp))    
    first.append(temp_value)
    first.append(temp_label)
    data.append(first) 
    return render_template('number_review.html',  max=17000,countries = ['Count reviews'],data = data)


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