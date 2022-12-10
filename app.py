# -*- coding: utf-8 -*-
"""
Created on Fri Dec 24 10:15:54 2021

@author: Nicolas Carval
"""
#%% Libraries and MongoDB connection

import numpy as np
from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
from ssh_pymongo import MongoSession
import pandas as pd
import json

import plotly
import plotly.express as px

# Connecting to MongoDB
session = MongoSession(
    "MESIIN592022-0055.westeurope.cloudapp.azure.com",
    port=22,
    user='administrateur',
    password='SuperPassword!1',
    uri="mongodb://MESIIN592022-0055:30000/")

movies = session.connection['movies']
#session.stop()

#%% STATIC PAGES

# method to send query to collection
def run_query(movies, pipeline):
    return movies.Movies_sharding_name.aggregate(pipeline,allowDiskUse=True)

#APPLICATION
app = Flask(__name__)

#Home page
@app.route('/')
def home():
    return render_template('index.html')

#User page
@app.route('/User',methods=['GET'])
def User():
    return render_template('User.html')

#Analyst page
@app.route('/Analyst',methods=['GET'])
def Analyst():
    # show the form, it wasn't submitted
    return render_template('Analyst.html')

# Results for User queries
@app.route('/Answer',methods=['GET'])
def Answer():
    return render_template('Answer.html')

# Results for Analyst queries
@app.route('/Answer2',methods=['GET'])
def Answer2():
    return render_template('Answer2.html')

@app.route('/Admin',methods=['GET'])
def Admin():
    return render_template('Admin.html')


#%% QUERIES
    

@app.route('/query1',methods=['POST'])
def query1():

    int_features = [str(x) for x in request.form.values()]
    pipeline = [{
        '$match': {
            'name': int_features[0]
        }
    }, {
        '$project': {
            'name': 1, 
            'year': 1, 
            'rank': 1, 
            'genre': 1, 
            'directors.first_name': 1, 
            'directors.last_name': 1, 
            'roles': 1
        }
    }]
    result= run_query(movies,pipeline)
    
    print(result._CommandCursor__data)
    output = pd.DataFrame(result._CommandCursor__data)
    output.drop(columns=["_id"], inplace = True)
    return render_template('Answer.html', tables=[output.to_html(classes='data')], titles=output.columns.values)

@app.route('/query2',methods=['POST'])
def query2():

    int_features = [str(x) for x in request.form.values()]
    pipeline = [
        {
            '$match': {
                'roles.first_name': int_features[0], 
                'roles.last_name': int_features[1]
            }
        }, {
            '$project': {
                'name': 1
            }
        }]
    result = run_query(movies, pipeline)
    
    print(result._CommandCursor__data)
    output = pd.DataFrame(result._CommandCursor__data)
    output.drop(columns=["_id"], inplace = True)
    return render_template('Answer.html', tables=[output.to_html(classes='data')], titles=output.columns.values)

@app.route('/query3',methods=['POST'])
def query3():

    int_features = [str(x) for x in request.form.values()]
    pipeline = [{
            '$match': {
                'roles.first_name': int_features[0], 
                'roles.last_name': int_features[1]
            }
        }, {
            '$unwind': {
                'path': '$roles', 
                'includeArrayIndex': 'string', 
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$group': {
                '_id': {
                    '_id': '$roles.actor_id', 
                    'first_name': '$roles.first_name', 
                    'last_name': '$roles.last_name'
                }, 
                'fieldN': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'fieldN': -1
            }
        }]
    result = run_query(movies, pipeline)

    print(result._CommandCursor__data)
    output = pd.DataFrame(result._CommandCursor__data)
    output.drop(columns=["_id"], inplace = True)
    return render_template('Answer.html', tables=[output.to_html(classes='data')], titles=output.columns.values)

@app.route('/query4',methods=['POST'])
def query4():

    int_features = [str(x) for x in request.form.values()]
    pipeline = [
        {
            '$match': {
                'directors.first_name': int_features[0], 
                'directors.last_name': int_features[1]
            }
        }, {
            '$project': {
                'path': '$directors.genres'
            }
        }, {
            '$limit': 1
        }
    ]
    result = run_query(movies, pipeline)
    print(result._CommandCursor__data)
    output = pd.DataFrame(result._CommandCursor__data)
    if output.shape[0] >=1:
        print("-------------")
        print(output.head())
        print("-------------")
        output.drop(columns=["_id"], inplace = True)        
        output = output.iloc[0]
        print(output[0][0])
        temp = {"genre":[x["genre"] for x in output[0][0]], "prob":[x["prob"] for x in output[0][0]]}
        output = pd.DataFrame(temp)
        output.sort_values(ascending=False, by = ["prob"],inplace=True)
    fig = px.bar(output, x='genre', y='prob', barmode='group', color_discrete_sequence = ["#A00C40"]*len(output))  
    fig.update_layout({'plot_bgcolor': 'rgba(62, 78, 80, 10)'})
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('Answer.html', graphJSON=graphJSON)

@app.route('/query5',methods=['POST'])
def query5():

    int_features = [str(x) for x in request.form.values()]
    pipeline = [{
        '$match': {
            'genre': int_features[0]
        }
    }, {
        '$unwind': {
            'path': '$directors', 
            'includeArrayIndex': 'string', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$directors', 
            'fieldN': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'fieldN': -1
        }
    }, {
        '$limit': 3
    }]
    result = run_query(movies, pipeline)
    print(result._CommandCursor__data)
    output = pd.DataFrame(result._CommandCursor__data)
    output.drop(columns=["_id"], inplace = True)
    return render_template('Answer2.html', tables=[output.to_html(classes='data')], titles=output.columns.values)

@app.route('/query7',methods=['POST'])
def query7():

    int_features = [int(x) for x in request.form.values()]
    pipeline = [{
        '$match': {
            'year': {
                '$gt': int_features[0]
            }, 
            'genre': {
                '$ne': None
            }, 
            'rank': {
                '$ne': None
            }
        }
    }, {
        '$unwind': {
            'path': '$genre', 
            'includeArrayIndex': 'string', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': {
                'genre': '$genre', 
                'year': '$year'
            }, 
            'fieldN': {
                '$avg': '$rank'
            }
        }
    }, {
        '$sort': {
            'fieldN': -1
        }
    }, {
        '$group': {
            '_id': '$_id.year', 
            'note_moyenne': {
                '$first': '$fieldN'
            }, 
            'top': {
                '$first': '$_id'
            }
        }
    }]
    result = run_query(movies, pipeline)
    print(result._CommandCursor__data)
    output = pd.DataFrame(result._CommandCursor__data)
    output["year"] = output["top"].apply(lambda x: x["year"])
    output["genre"] = output["top"].apply(lambda x: x["genre"])
    output.drop(columns=["_id",'top'], inplace = True)
    output.sort_values(by=["year"], inplace = True)
    output.reset_index(drop=True)
    output = output[["year","genre","note_moyenne"]]
    print(output)
    fig = px.bar(output, x='note_moyenne', y="year", barmode='group', color="genre", orientation="h")  
    fig.update_layout({'plot_bgcolor': 'rgba(62, 78, 80, 10)','xaxis_range':[0,10], 'yaxis':{"tickmode" : 'array',
        "tickvals" : output.year.values}})
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('Answer2.html', graphJSON=graphJSON)

@app.route('/query8',methods=['POST'])
def query8():
    pipeline = [{
        '$unwind': {
            'path': '$roles', 
            'includeArrayIndex': 'string', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$roles.actor_id', 
            'last_name': {
                '$first': '$roles.last_name'
            }, 
            'first': {
                '$first': '$roles.first_name'
            }, 
            'max': {
                '$max': '$year'
            }, 
            'min': {
                '$min': '$year'
            }
        }
    }, {
        '$addFields': {
            'dif': {
                '$subtract': [
                    '$max', '$min'
                ]
            }
        }
    }, {
        '$sort': {
            'dif': -1
        }
    },{
        '$limit': 3
    }]
    result = run_query(movies, pipeline)
    print(result._CommandCursor__data)
    output = pd.DataFrame(result._CommandCursor__data)
    output.drop(columns=["_id"], inplace = True)
    print(output)

    return render_template('Answer2.html',tables=[output.to_html(classes='data')], titles=output.columns.values)


@app.route('/Admin',methods=['POST'])
def Admin_query():
    result = movies.Movies_sharding_name.index_information()
    aha = [result[x]["key"][0][0] for x,v in result.items()]
    rep = " - ".join(aha)
    print(rep)

    return render_template('Admin.html',keys = rep)

#%% MAIN
if __name__ == "__main__":
    app.run(host='localhost', port=8989)