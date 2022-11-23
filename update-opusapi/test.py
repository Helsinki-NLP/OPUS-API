import urllib.request
import yaml
import re
import sqlite3

def read_url(url):
    return urllib.request.urlopen(url).read().decode('utf-8').split('\n')

def read_url_yaml(url):
    raw = urllib.request.urlopen(url).read().decode('utf-8')
    data = yaml.full_load(raw)
    return data

def create_table(cur):
    create_opusfile_table = '''CREATE TABLE IF NOT EXISTS opusfile (
    id integer PRIMARY KEY,
    source text,
    target text,
    corpus text,
    preprocessing text,
    version text,
    url text,
    size integer,
    documents integer,
    alignment_pairs integer,
    source_tokens integer,
    target_tokens integer,
    latest text
    );'''
    cur.execute(create_opusfile_table)

def execute_sql(cur, opusfile):
    sql = '''INSERT INTO opusfile(source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'''
    cur.execute(sql, opusfile)

def get_lang_info(name, data, data_type):
        source, target = name, ''
        if data_type == 'bitexts':
            source, target = name.split('-')
        documents = ''
        if data_type in ['bitexts', 'monolingual']:
            documents = data['files']
        if data_type in ['bitexts', 'moses']:
            alignment_pairs = data['alignments']
        elif data_type == 'tmx':
            alignment_pairs = data['translation units']
        elif data_type == 'monolingual':
            alignment_pairs = data['sentences']
        if data_type == 'monolingual':
            source_tokens = data['tokens']
            target_tokens = ''
        else:
            source_tokens = data['source language tokens']
            target_tokens = data['target language tokens']

        return source, target, documents, alignment_pairs, source_tokens, target_tokens

def get_size_url_prep(data, data_type):
    if data_type in ['tmx', 'moses']:
        size = int(int(data['download size'])/1024)
        url = data['download url']
    elif data_type in ['bitexts', 'monolingual']:
        size = int(int(data['size'])/1024)
        url = data['url']
    preprocessing = url.split('/')[-2]

    return size, url, preprocessing

def get_tmx_entries(corpus, version, latest, tmx, cur):
    for item in tmx:
        source, target, documents, alignment_pairs, source_tokens, target_tokens = get_lang_info(item, tmx[item], 'tmx')
        size, url, preprocessing = get_size_url_prep(tmx[item], 'tmx')
        opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest)
        execute_sql(cur, opusfile)

def get_moses_entries(corpus, version, latest, moses, cur):
    for item in moses:
        source, target, documents, alignment_pairs, source_tokens, target_tokens = get_lang_info(item, moses[item], 'moses')
        size, url, preprocessing = get_size_url_prep(moses[item], 'moses')
        opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest)
        execute_sql(cur, opusfile)

def get_monolingual_entries(corpus, version, latest, monolingual, cur):
    for item in monolingual:
        source, target, documents, alignment_pairs, source_tokens, target_tokens = get_lang_info(item, monolingual[item], 'monolingual')
        for entry in monolingual[item]['downloads'].items():
            size, url, preprocessing = get_size_url_prep(entry[1], 'monolingual')
            opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest)
            execute_sql(cur, opusfile)

def get_bitext_entries(corpus, version, latest, bitexts, cur):
    for item in bitexts:
        source, target, documents, alignment_pairs, source_tokens, target_tokens = get_lang_info(item, bitexts[item], 'bitexts')
        for entry in bitexts[item]['downloads'].items():
            # exclude monolingual files, they are added in the monolingual phase
            if 'language' not in entry[0]:
                size, url, preprocessing = get_size_url_prep(entry[1], 'bitexts')
                opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest)
                execute_sql(cur, opusfile)

def main():
    con = sqlite3.connect('opusdata.db')
    cur = con.cursor()

    create_table(cur)

    URL_BASE = 'https://raw.githubusercontent.com/Helsinki-NLP/OPUS/main/corpus/'
    index_info = read_url(URL_BASE + 'index-info.txt')

    i = 0
    for info in index_info:
        info_s = info.split('/')
        if len(info_s) == 2:
            gen_info = read_url_yaml(URL_BASE + info)
            corpus = gen_info['name']
            i += i
            if i == 3: break
            print(f'Processing corpus {corpus}')
            latest_v = gen_info['latest release']
        elif len(info_s) == 3:
            version = info_s[1]
            latest = 'False'
            if version == latest_v:
                latest = 'True'
            stats = info.replace('info', 'statistics')
            corpus_data = read_url_yaml(URL_BASE + stats)
            get_bitext_entries(corpus, version, latest, corpus_data['bitexts'], cur)
            get_monolingual_entries(corpus, version, latest, corpus_data['monolingual'], cur)
            get_moses_entries(corpus, version, latest, corpus_data['moses'], cur)
            get_tmx_entries(corpus, version, latest, corpus_data['tmx'], cur)

    con.commit()
    con.close()

if __name__ == "__main__":
    main()
