import os
import sqlite3

from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

DB_FILE = os.environ["OPUSAPI_DB"]

def clean_up_parameters(parameters):
    remove = []
    valid_keys = ['corpus', 'id', 'latest', 'preprocessing', 'source', 'target', 'version', 'corpora', 'languages']
    for key, value in parameters.items():
        if key not in valid_keys:
            remove.append(key)
            continue
        value = value.replace('"', '')
        value = value.replace('\'', '')
        parameters[key] = value
    for key in remove:
        del parameters[key]
    return parameters

def run_query(sql_command):
    conn = sqlite3.connect(DB_FILE)
    query = conn.execute(sql_command)
    value_list = query.fetchall()
    keys = [i[0] for i in query.description]
    conn.close()
    return keys, value_list

def run_default_query(parameters, suffix=''):
    sql_command = 'SELECT * FROM opusfile WHERE '+' AND '.join([f'{k} = "{v}"' for k, v in parameters.items()]) + suffix
    keys, value_list = run_query(sql_command)
    ret = [{k: v for k, v in zip(keys,values)} for values in value_list]
    return ret

def run_corpora_query(parameters):
    sql_command = 'SELECT DISTINCT corpus FROM opusfile'
    if len(parameters) > 0:
        sql_command = sql_command+' WHERE '+' AND '.join([f'{k} = "{v}"' for k, v in parameters.items()])
    _, value_list = run_query(sql_command)
    values = [v[0] for v in value_list]
    return values

def run_languages_query(parameters):
    sql_command = 'SELECT DISTINCT source FROM opusfile '
    if len(parameters) > 0:
        source = parameters.get('source')
        if source:
            sql_command = 'SELECT DISTINCT target FROM opusfile where '+' AND '.join([f'{k} = "{v}"' for k, v in parameters.items()]) + f' AND target != "{source}" AND target != "" UNION SELECT DISTINCT source FROM opusfile '
            parameters['target'] = parameters['source']
            del parameters['source']
        sql_command = sql_command + 'WHERE '
    sql_command = sql_command + ' AND '.join([f'{k} = "{v}"' for k, v in parameters.items()])
    _, value_list = run_query(sql_command)
    values = [v[0] for v in value_list]
    return values

@app.route('/')
def opusapi():
    parameters = request.args.copy()
    parameters = clean_up_parameters(parameters)

    source = parameters.get('source')
    target = parameters.get('target')

    if source and target:
        sou_tar = sorted([source, target])
        parameters['source'] = sou_tar[0]
        parameters['target'] = sou_tar[1]

    if len(parameters) == 0:
        #baseurl = 'http://opus.nlpl.eu/opusapi/'
        baseurl='http://127.0.0.1:5000/'
        return render_template('opusapi.html', baseurl=baseurl)

    if 'corpora' in parameters.keys():
        del parameters['corpora']
        ret = run_corpora_query(parameters)
        return jsonify(corpora=ret)

    if 'languages' in parameters.keys():
        del parameters['languages']
        ret = run_languages_query(parameters)
        return jsonify(languages=ret)

    version = parameters.get('version')
    if version and version == 'latest':
        parameters['latest'] = 'True'
        del parameters['version']

    ret = []

    preprocessing = parameters.get('preprocessing')

    if preprocessing in ['xml', 'raw', 'parsed']:
        a_parameters = parameters.copy()
        a_parameters['preprocessing'] = 'xml'
        ret = ret + run_default_query(a_parameters, suffix=' AND target != ""')
        languages = set()
        for item in ret:
            languages.add(item['source'])
            languages.add(item['target'])

        parameters['target'] = ''
        for language in languages:
            parameters['source'] = language
            ret = ret + run_default_query(parameters)
    else:
        ret = run_default_query(parameters)

    return jsonify(corpora=ret)
