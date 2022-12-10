# -*- coding: utf-8 -*-
"""
Created on Fri Dec 24 10:15:54 2021

@author: Nicolas Carval
"""

import numpy as np
from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
from ssh_pymongo import MongoSession
import pandas as pd

session = MongoSession(
    "MESIIN592022-0055.westeurope.cloudapp.azure.com",
    port=22,
    user='administrateur',
    password='SuperPassword!1',
    uri="mongodb://MESIIN592022-0055:30000/")

movies = session.connection['movies']
#movies_year=session.connection["movies"]["Movies_sharding_year"]
#session.stop()

def run_query(movies, pipeline):
    return movies.Movies_sharding_name.aggregate(pipeline,allowDiskUse=True)

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/User',methods=['GET'])
def User():
    # show the form, it wasn't submitted
    return render_template('User.html')

@app.route('/Analyst',methods=['GET'])
def Analyst():
    # show the form, it wasn't submitted
    return render_template('Analyst.html')

@app.route('/Answer',methods=['GET'])
def Answer():
    # show the form, it wasn't submitted
    return render_template('Answer.html')




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
    output.drop(columns=["_id"], inplace = True)
    return render_template('Answer.html',tables=[output.to_html(classes='data')], titles=output.columns.values)

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
    return render_template('Answer2.html', tables=[output.to_html(classes='data')], titles=output.columns.values)

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

if __name__ == "__main__":
    app.run(host='localhost', port=8989)