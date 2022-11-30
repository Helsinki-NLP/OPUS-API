import urllib.request
import yaml
import sqlite3
import logging

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

def get_lang_info(name, data, data_type, info):
    source, target, documents, alignment_pairs, source_tokens, target_tokens = '', '', '', '', '', ''
    source = name
    if data_type in ['bitexts', 'moses', 'tmx']:
        names = name.split('-')
        if len(names) != 2:
            logging.warning(f'{info} {data_type} {name}: cannot split name "{name}" into two language codes')
        else:
            source, target = names
    documents = ''
    if data_type in ['bitexts', 'monolingual']:
        documents = data.get('files', '')
        if documents == '':
            logging.warning(f'{info} {data_type} {name} is missing "files"')
    if data_type in ['bitexts', 'moses']:
        alignment_pairs = data.get('alignments', '')
        if alignment_pairs == '':
            logging.warning(f'{info} {data_type} {name} is missing "alignments"')
    elif data_type == 'tmx':
        alignment_pairs = data.get('translation units', '')
        if alignment_pairs == '':
            logging.warning(f'{info} {data_type} {name} is missing "translation units"')
    elif data_type == 'monolingual':
        alignment_pairs = data.get('sentences', '')
        if alignment_pairs == '':
            logging.warning(f'{info} {data_type} {name} is missing "sentences"')
    if data_type == 'monolingual':
        source_tokens = data.get('tokens', '')
        if source_tokens == '':
            logging.warning(f'{info} {data_type} {name} is missing "tokens"')
        target_tokens = ''
    else:
        source_tokens = data.get('source language tokens', '')
        if source_tokens == '':
            logging.warning(f'{info} {data_type} {name} is missing "source language tokens"')
        target_tokens = data.get('target language tokens', '')
        if target_tokens == '':
            logging.warning(f'{info} {data_type} {name} is missing "target language tokens"')

    return source, target, documents, alignment_pairs, source_tokens, target_tokens

def get_size_url_prep(name, data, data_type, info):
    size, url, preprocessing = '','',''
    if data_type in ['tmx', 'moses']:
        size = data.get('download size', '')
        if size == '':
            logging.warning(f'{info} {data_type} {name} is missing "download size"')
        else:
            size = int(int(size)/1024)
        url = data.get('download url', '')
        if url == '':
            logging.warning(f'{info} {data_type} {name} is missing "download url"')
    elif data_type in ['bitexts', 'monolingual']:
        size = data.get('size', '')
        if size == '':
            logging.warning(f'{info} {data_type} {name} is missing "size"')
        else:
            size = int(int(size)/1024)
        url = data.get('url', '')
        if url == '':
            logging.warning(f'{info} {data_type} {name} is missing "url"')

    pre_step = url.split('/')
    if len(pre_step) < 2:
        logging.warning(f'{info} {data_type} {name}: cannot find preprocessing from url "{url}"')
    else:
        preprocessing = pre_step[-2]

    return size, url, preprocessing

def get_tmx_entries(corpus, version, latest, tmx, cur, info):
    for item in tmx:
        source, target, documents, alignment_pairs, source_tokens, target_tokens = get_lang_info(item, tmx[item], 'tmx', info)
        size, url, preprocessing = get_size_url_prep(item, tmx[item], 'tmx', info)
        opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest)
        execute_sql(cur, opusfile)

def get_moses_entries(corpus, version, latest, moses, cur, info):
    for item in moses:
        source, target, documents, alignment_pairs, source_tokens, target_tokens = get_lang_info(item, moses[item], 'moses', info)
        size, url, preprocessing = get_size_url_prep(item, moses[item], 'moses', info)
        opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest)
        execute_sql(cur, opusfile)

def get_monolingual_entries(corpus, version, latest, monolingual, cur, info):
    for item in monolingual:
        source, target, documents, alignment_pairs, source_tokens, target_tokens = get_lang_info(item, monolingual[item], 'monolingual', info)
        for entry in monolingual[item]['downloads'].items():
            size, url, preprocessing = get_size_url_prep(item, entry[1], 'monolingual', info)
            opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest)
            execute_sql(cur, opusfile)

def get_bitext_entries(corpus, version, latest, bitexts, cur, info):
    for item in bitexts:
        source, target, documents, alignment_pairs, source_tokens, target_tokens = get_lang_info(item, bitexts[item], 'bitexts', info)
        for entry in bitexts[item]['downloads'].items():
            # exclude monolingual files, they are added in the monolingual phase
            if 'language' not in entry[0]:
                size, url, preprocessing = get_size_url_prep(item, entry[1], 'bitexts', info)
                opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest)
                execute_sql(cur, opusfile)

def main():
    logging.basicConfig(filename='error.log', level=logging.ERROR,
            format='%(asctime)s %(levelname)s:%(name)s: %(message)s', datefmt='%x %X')

    con = sqlite3.connect('opusdata.db')
    cur = con.cursor()

    create_table(cur)

    URL_BASE = 'https://raw.githubusercontent.com/Helsinki-NLP/OPUS/main/corpus/'
    index_info = read_url(URL_BASE + 'index-info.txt')

    for info in index_info:
            info_s = info.split('/')
            if len(info_s) == 2:
                try:
                    gen_info = read_url_yaml(URL_BASE + info)
                except (yaml.reader.ReaderError, urllib.error.HTTPError) as e:
                    logging.error(f'{info}, {type(e).__name__}: {e}')
                    continue
                corpus = gen_info.get('name')
                if not corpus:
                    logging.warning(f'{info}, corpus name missing')
                print(f'Processing corpus {corpus}')
                latest_v = gen_info.get('latest release')
                if not latest_v:
                    logging.warning(f'{info}, latest release missing')
            elif len(info_s) == 3:
                version = info_s[1]
                latest = 'False'
                if version == latest_v:
                    latest = 'True'
                stats = info.replace('info.yaml', 'statistics.yaml')
                try:
                    corpus_data = read_url_yaml(URL_BASE + stats)
                except (yaml.reader.ReaderError, urllib.error.HTTPError) as e:
                    logging.error(f'{stats}, {type(e).__name__}: {e}')
                    continue

                get_entries = {'bitexts': get_bitext_entries,
                                'monolingual': get_monolingual_entries,
                                'moses': get_moses_entries,
                                'tmx': get_tmx_entries}

                if not corpus_data:
                    logging.error(f'{info}, corpus_data is empty')
                    continue

                for item in get_entries.keys():
                    sub_data = corpus_data.get(item)
                    if sub_data:
                        get_entries[item](corpus, version, latest, sub_data, cur, info)
                    else:
                        logging.warning(f'{info}, {item} data missing')

    con.commit()
    con.close()

if __name__ == "__main__":
    main()
