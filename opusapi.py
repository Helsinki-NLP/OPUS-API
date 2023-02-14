from flask import Flask, render_template, request, jsonify

from db_operations import clean_up_parameters, run_corpora_query, run_languages_query, get_corpora

app = Flask(__name__)

@app.route('/')
def opusapi():
    parameters = request.args.copy()
    parameters = clean_up_parameters(parameters)

    if len(parameters) == 0:
        #baseurl = 'http://opus.nlpl.eu/opusapi/'
        baseurl='http://127.0.0.1:5000/'
        return render_template('opusapi.html', baseurl=baseurl)

    if 'corpora' in parameters.keys():
        return jsonify(corpora=run_corpora_query(parameters))

    if 'languages' in parameters.keys():
        return jsonify(languages=run_languages_query(parameters))

    return jsonify(corpora=get_corpora(parameters))
