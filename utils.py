import csv
import pandas as pd
import json
import pyathenajdbc

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
