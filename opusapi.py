import os
from flask import Flask, render_template, request, jsonify

from opustools import DbOperations

app = Flask(__name__)

@app.route('/')
def opusapi():
    dbo = DbOperations(db_file=os.environ['OPUSAPI_DB'])

    parameters = request.args.copy()
    parameters = dbo.clean_up_parameters(parameters)

    if len(parameters) == 0:
        #baseurl = 'http://opus.nlpl.eu/opusapi/'
        baseurl='http://127.0.0.1:5000/'
        return render_template('opusapi.html', baseurl=baseurl)

    if 'corpora' in parameters.keys():
        return jsonify(corpora=dbo.run_corpora_query(parameters))

    if 'languages' in parameters.keys():
        return jsonify(languages=dbo.run_languages_query(parameters))

    return jsonify(corpora=dbo.get_corpora(parameters))
