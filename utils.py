import csv
import pandas as pd
import json
import os
import time
from utils import *
from flask import Flask, render_template
from python_jobs.listings_pre import *
import boto3
def get_reviews():
    data = []
    with open('done/reviews.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            buffer = []
            buffer.append(row[2])
            buffer.append(row[3])
            buffer.append(row[4])
            data.append(buffer)
    data = data[-20:]
    return data

def get_hosts_data():
    data = []
    df = pd.read_csv('done/reviews.csv')
    del df['listing_id']
    del df['clean_comment']
    grouped_df = df.groupby(["reviewer_name","reviewer_id"])
    grouped_and_summed = grouped_df.sum()
    grouped_and_summed_desc = grouped_and_summed.sort_values(by=['score'], ascending=True)
    grouped_and_summed = grouped_and_summed.sort_values(by=['score'], ascending=False)
    #print(grouped_and_summed)    
    top_hosts = grouped_and_summed.head(10).to_json(orient ='index')
    worst_hosts = grouped_and_summed_desc.head(10).to_json(orient ='index')
    
    worsts = json.loads(worst_hosts)
    bests = json.loads(top_hosts)
    worstss = []
    for key, value in worsts.items():
        temp = [key,worsts[key]]
        worstss.append(temp)
    bestss = []
    for key, value in bests.items():
        temp = [key,bests[key]]
        bestss.append(temp)
    return bestss,worstss


def get_worst_and_bests_hosts(country,athena):
    response = athena.start_query_execution(
                QueryString = """SELECT reviewer_name,sum(score)as score FROM "grupo5_db"."reviews_"""+country+"""" group by reviewer_name order by score desc limit 10""",
                    QueryExecutionContext={
                    'Database': 'grupo5_db'
                    },
                    ResultConfiguration={
                    'OutputLocation': 's3://aypmd-grupo5/query_results',
                    }
            )
    query_execution_id = response['QueryExecutionId']
    time.sleep(2)
    responsee = athena.get_query_results(QueryExecutionId = query_execution_id)
    output = []##the lists ready for bests/worsts
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
    worst_name = list(map(lambda x: x[0],output))
    worst_score = list(map(lambda x: x[1],output))

    bar_labels_worst=worst_name
    bar_values_worst=worst_score
    ######
    response = athena.start_query_execution(
                QueryString = """SELECT reviewer_name,sum(score)as score FROM "grupo5_db"."reviews_"""+country+"""" where LENGTH(reviewer_name)<13 group by reviewer_name order by score asc limit 10""",
                    QueryExecutionContext={
                    'Database': 'grupo5_db'
                    },
                    ResultConfiguration={
                    'OutputLocation': 's3://aypmd-grupo5/query_results',
                    }
            )
    query_execution_id = response['QueryExecutionId']
    time.sleep(2)
    responsee = athena.get_query_results(QueryExecutionId = query_execution_id)
    output = []##the lists ready for bests/worsts
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
    best_name = list(map(lambda x: x[0],output))
    best_score = list(map(lambda x: float(x[1])*(-1.0),output))

    bar_labels_best=best_name
    bar_values_best=best_score
    
    
    
    return bar_labels_worst,bar_values_worst ,bar_labels_best,bar_values_best


def get_score_neighborhood(country,athena):
    response = athena.start_query_execution(
                QueryString="""select sum(r_paris.score) as score, l_paris."neighbourhood_cleansed"
from "grupo5_db"."reviews_"""+country+"""" r_paris
inner join "grupo5_db"."listings_"""+country+"""" l_paris
on cast(l_paris."id" as varchar(10)) = listing_id
group by l_paris."neighbourhood_cleansed"
order by score desc""",
                    QueryExecutionContext={
                    'Database': 'grupo5_db'
                    },
                    ResultConfiguration={
                    'OutputLocation': 's3://aypmd-grupo5/query_results',
                    }
            )
    query_execution_id = response['QueryExecutionId']
    time.sleep(4)
    responsee = athena.get_query_results(QueryExecutionId = query_execution_id)
    output = []##the lists ready for bests/worsts
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
    worst_name = list(map(lambda x: x[0],output))
    worst_score = list(map(lambda x: x[1],output))

    bar_labels_worst=worst_name[:10]
    bar_values_worst=worst_score[:10]
    return bar_labels_worst,bar_values_worst 

def get_score_beds(country,athena):
    response = athena.start_query_execution(
                QueryString="""select sum(r_paris.score) as score, l_paris."beds"
from "grupo5_db"."reviews_"""+country+"""" r_paris
inner join "grupo5_db"."listings_"""+country+"""" l_paris
on cast(l_paris."id" as varchar(10)) = listing_id
group by l_paris."beds"
order by score desc""",
                    QueryExecutionContext={
                    'Database': 'grupo5_db'
                    },
                    ResultConfiguration={
                    'OutputLocation': 's3://aypmd-grupo5/query_results',
                    }
            )
    query_execution_id = response['QueryExecutionId']
    time.sleep(4)
    responsee = athena.get_query_results(QueryExecutionId = query_execution_id)
    output = []##the lists ready for bests/worsts
    cont = 0
    for i in responsee['ResultSet']['Rows']:
        if cont != 0:
            data = i['Data']
            temp = []
            for j in data:
                try:
                    temp.append(j['VarCharValue'])
                except Exception:
                    temp.append('Error')
            output.append(temp)
        else:
            cont+=1
    worst_name = list(map(lambda x: x[0],output))
    worst_score = list(map(lambda x: x[1],output))

    bar_labels_worst=worst_name[:10]
    bar_values_worst=worst_score[:10]
    return bar_labels_worst,bar_values_worst 

def exequte_query(query,athena):
    response = athena.start_query_execution(
                QueryString=query,
                    QueryExecutionContext={
                    'Database': 'grupo5_db'
                    },
                    ResultConfiguration={
                    'OutputLocation': 's3://aypmd-grupo5/query_results',
                    }
            )
    query_execution_id = response['QueryExecutionId']
    time.sleep(6)
    responsee = athena.get_query_results(QueryExecutionId = query_execution_id)
    output = []##the lists ready for bests/worsts
    for i in responsee['ResultSet']['Rows']:
        data = i['Data']
        temp = []
        for j in data:
            try:
                temp.append(j['VarCharValue'])
            except Exception:
                temp.append('Error')
        output.append(temp)
    
    return output 