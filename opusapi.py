from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine
import os

app = Flask(__name__)

opusapi_connection = create_engine('sqlite:///'+os.environ["OPUSAPI_DB"])
    
def make_sql_command(parameters, direction):
    sql_command = "SELECT * FROM opusfile WHERE "
    
    so = parameters[0][1]
    ta = parameters[1][1]

    ret_param = []
    
    if direction:
        sql_command += "((source='"+so+"' AND target='"+ta+"') OR (source='"+so+\
                       "' AND target='') OR (source='"+ta+"' AND target='')) AND "
        parameters[0] = ("source", "#EMPTY#")
        parameters[1] = ("target", "#EMPTY#")
    
    if ta == "#EMPTY#" and so != "#EMPTY#":
        sql_command += "((source='"+so+"') or (target='"+so+"')) AND "
        parameters[0] = ("source", "#EMPTY#")
     
    for i in parameters:
        if i[1] != "#EMPTY#":
            sql_command += i[0] + "= ? AND "
            ret_param.append(i[1])

    sql_command = sql_command.strip().split(" ")
    sql_command = " ".join(sql_command[:-1])

    if parameters[3][1] in ["raw", "parsed"]:
        if direction:
            sql_command += " UNION SELECT * FROM opusfile WHERE source='"+so+"' AND target='"+ta+"' AND "        
        else:
            sql_command += " UNION SELECT * FROM opusfile WHERE ((source='"+so+"' AND target!='') OR (source!='' AND target='"+so+"')) AND "        
        for i in parameters:
            if i[0] == "preprocessing":
                sql_command += "preprocessing='xml' AND "
            elif i[1] != "#EMPTY#":
                sql_command += i[0] + "= ? AND "
                ret_param.append(i[1])
        sql_command = sql_command.strip().split(" ")
        sql_command = " ".join(sql_command[:-1])

    return sql_command, tuple(ret_param)
    
def opusEntry(keys, values):
    entry = dict(zip(tuple(keys), values))
    entry["url"] = "https://object.pouta.csc.fi/OPUS-"+entry["url"]
    return entry

def getLanguages(sou, cor):
    if sou == "#EMPTY#" and cor == "#EMPTY#":
        sql_command = "SELECT DISTINCT source FROM opusfile ORDER BY source"
    elif sou != "#EMPTY#" and cor == "#EMPTY#":
        sql_command = "SELECT source FROM opusfile WHERE target='"+sou+"' AND source!='"+sou+"' UNION SELECT target FROM opusfile WHERE source='"+sou+"' AND target!='' AND target!='"+sou+"';"
    elif sou == "#EMPTY#" and cor != "#EMPTY#":
        sql_command = "SELECT source FROM opusfile WHERE corpus='"+cor+"' UNION SELECT target FROM opusfile WHERE corpus='"+cor+"' AND target!='""';"
    elif sou != "#EMPTY" and cor != "#EMPTY#":
        sql_command = "SELECT source FROM opusfile WHERE corpus='"+cor+"' AND target='"+sou+"' AND source!='"+sou+"' UNION SELECT target FROM opusfile WHERE corpus='"+cor+"' AND source='"+sou+"' AND target!='' AND target!='"+sou+"';"
    conn = opusapi_connection.connect()
    query = conn.execute(sql_command)
    return [i[0] for i in query.cursor]

def getCorpora(sou, tar):
    if sou == "#EMPTY#" and tar == "#EMPTY#":
        sql_command = "SELECT DISTINCT corpus FROM opusfile ORDER BY corpus"
    elif sou != "#EMPTY#" and tar == "#EMPTY#":
        sql_command = "SELECT DISTINCT corpus FROM opusfile WHERE source='"+sou+"' ORDER BY corpus"
    elif sou != "#EMPTY#" and tar != "#EMPTY#":
        sql_command = "SELECT DISTINCT corpus FROM opusfile WHERE source='"+sou+"' AND target='"+tar+"' ORDER BY corpus"
    conn = opusapi_connection.connect()
    query = conn.execute(sql_command)
    return [i[0] for i in query.cursor]

def submitCommand(sql_command, params, direction):
    conn = opusapi_connection.connect()
    query = conn.execute(sql_command, params)

    ret = [opusEntry(query.keys(), i) for i in query.cursor]
    if direction:
        found_corp = set()
        for entry in ret:
            if entry['source'] != '' and entry['target'] != '':
                found_corp.add(entry['corpus'])
        new_ret = []
        for entry in ret:
            if entry['corpus'] in found_corp:
                new_ret.append(entry)
        ret = new_ret

    return ret

def addRelatedMonoData(ret, parameters):
    languages = set()
    ret_param = []
    for i in ret:
        if i['source'] != parameters[0][1]:
            languages.add(i['source'])
        if i['target'] != '' and i['target'] != parameters[0][1]:
            languages.add(i['target'])
    lang_comm = " OR ".join(["source='"+lan+"'" for lan in languages])
    sql_command = "SELECT * FROM opusfile WHERE ("+lang_comm+") AND target='' AND "        
    for i in parameters[2:]:
        if i[1] != "#EMPTY#":
            sql_command += i[0] + "= ? AND "
            ret_param.append(i[1])
    sql_command = sql_command.strip().split(" ")
    sql_command = " ".join(sql_command[:-1])

    conn = opusapi_connection.connect()
    query = conn.execute(sql_command, ret_param)

    return ret + [opusEntry(query.keys(), i) for i in query.cursor]
   
@app.route('/')
def opusapi():
    source = request.args.get('source', '#EMPTY#', type=str)
    target = request.args.get('target', '#EMPTY#', type=str)
    corpus = request.args.get('corpus', '#EMPTY#', type=str)
    preprocessing = request.args.get('preprocessing', '#EMPTY#', type=str)
    version = request.args.get('version', '#EMPTY#', type=str)
    languages = request.args.get('languages', False, type=bool)
    corpora = request.args.get('corpora', False, type=bool)

    sou_tar = [source, target]
    sou_tar.sort()

    direction = True
    if "#EMPTY#" in sou_tar or "" in sou_tar:
        sou_tar.sort(reverse=True)
        direction = False

    latest = "#EMPTY#"
    if version == "latest":
        version = "#EMPTY#"
        latest = "True"
    
    parameters = [("source", sou_tar[0]), ("target", sou_tar[1]), ("corpus", corpus),
                  ("preprocessing", preprocessing), ("version", version), 
                  ("latest", latest)]

    sql_command, params = make_sql_command(parameters.copy(), direction)

    if languages:
        return jsonify(languages=getLanguages(sou_tar[0], corpus))
    if corpora:
        return jsonify(corpora=getCorpora(sou_tar[0], sou_tar[1]))
    if params == ():
        return render_template('opusapi.html')

    ret = submitCommand(sql_command, params, direction)

    if len(ret)>0 and not direction and preprocessing in ["xml", "raw", "parsed", "#EMPTY#"]:
        ret = addRelatedMonoData(ret, parameters)

    return jsonify(corpora=ret)
