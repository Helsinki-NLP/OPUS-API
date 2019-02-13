from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine

app = Flask(__name__)

opusapi_connection = create_engine('sqlite:////var/www/opusdata.db')
    
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

    if direction and parameters[3][1] not in ["dic", "moses", "smt", "xml", "tmx", "wordalign"]:
        sql_command += " UNION SELECT * FROM opusfile WHERE source='"+so+"' AND target='"+ta+"' AND "        
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
    
@app.route('/')
def opusapi():
    source = request.args.get('source', '#EMPTY#', type=str)
    target = request.args.get('target', '#EMPTY#', type=str)
    corpus = request.args.get('corpus', '#EMPTY#', type=str)
    preprocessing = request.args.get('preprocessing', '#EMPTY#', type=str)
    version = request.args.get('version', '#EMPTY#', type=str)

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

    sql_command, params = make_sql_command(parameters, direction)
    if sql_command == "SELECT * FROM opusfile":
        #sql_command = sql_command + " LIMIT 10"
        return render_template("opusapi.html")

    conn = opusapi_connection.connect()
    query = conn.execute(sql_command, params)

    ret = [opusEntry(query.keys(), i) for i in query.cursor]

    return jsonify(corpora=ret)

