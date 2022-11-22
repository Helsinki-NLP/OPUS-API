import urllib.request
import yaml
import re
import pprint

pp = pprint.PrettyPrinter()

def read_url(url):
    return urllib.request.urlopen(url).read().decode('utf-8').split('\n')

def read_url_yaml(url):
    raw = urllib.request.urlopen(url).read().decode('utf-8')
    data = yaml.full_load(raw)
    return data

def get_tmx_entries(corpus, version, latest, tmx):
    for item in tmx:
        src, trg = item, ''
        documents = ''
        alignment_pairs = tmx[item]['translation units']
        source_tokens = tmx[item]['source language tokens']
        target_tokens = tmx[item]['target language tokens']
        size = tmx[item]['download size']
        url = tmx[item]['download url']
        preprocessing = url.split('/')[-2]
        print(f'corpus: {corpus}, version: {version}, src: {src}, trg: {trg}, documents: {documents}, '
                f'alignment_pairs: {alignment_pairs}, source_tokens: {source_tokens}, target_tokens: '
                f'{target_tokens}, latest: {latest}, size: {size}, preprocessing: {preprocessing}, url: {url}')
        input()

def get_moses_entries(corpus, version, latest, moses):
    for item in moses:
        src, trg = item, ''
        documents = ''
        alignment_pairs = moses[item]['alignments']
        source_tokens = moses[item]['source language tokens']
        target_tokens = moses[item]['target language tokens']
        size = moses[item]['download size']
        url = moses[item]['download url']
        preprocessing = url.split('/')[-2]
        print(f'corpus: {corpus}, version: {version}, src: {src}, trg: {trg}, documents: {documents}, '
                f'alignment_pairs: {alignment_pairs}, source_tokens: {source_tokens}, target_tokens: '
                f'{target_tokens}, latest: {latest}, size: {size}, preprocessing: {preprocessing}, url: {url}')
        input()

def get_monolingual_entries(corpus, version, latest, monolingual):
    for item in monolingual:
        src, trg = item, ''
        documents = monolingual[item]['files']
        alignment_pairs = monolingual[item]['sentences']
        source_tokens = monolingual[item]['tokens']
        target_tokens = ''
        for entry in monolingual[item]['downloads'].items():
            size = entry[1]['size']
            url = entry[1]['url']
            preprocessing = url.split('/')[-2]
            print(f'corpus: {corpus}, version: {version}, src: {src}, trg: {trg}, documents: {documents}, '
                    f'alignment_pairs: {alignment_pairs}, source_tokens: {source_tokens}, target_tokens: '
                    f'{target_tokens}, latest: {latest}, size: {size}, preprocessing: {preprocessing}, url: {url}')
            input()

def get_bitext_entries(corpus, version, latest, bitexts):
    for item in bitexts:
        src, trg = item.split('-')
        documents = bitexts[item]['files']
        alignment_pairs = bitexts[item]['alignments']
        source_tokens = bitexts[item]['source language tokens']
        target_tokens = bitexts[item]['target language tokens']
        for entry in bitexts[item]['downloads'].items():
            if 'language' not in entry[0]:
                size = entry[1]['size']
                url = entry[1]['url']
                preprocessing = url.split('/')[-2]
                print(f'corpus: {corpus}, version: {version}, src: {src}, trg: {trg}, documents: {documents}, '
                        f'alignment_pairs: {alignment_pairs}, source_tokens: {source_tokens}, target_tokens: '
                        f'{target_tokens}, latest: {latest}, size: {size}, preprocessing: {preprocessing}, url: {url}')
                input()

def get_opus_entries(corpus, version, latest, corpus_data):
    for name, data in corpus_data.items():
        print(name)
        for item in data:
            src, trg = '', ''
            src, trg = item.split('-')
            documents = data[item]['files']
            alignment_pairs = data[item]['alignments']
            source_tokens = data[item]['source language tokens']
            target_tokens = data[item]['target language tokens']
            for entry in data[item]['downloads'].items():
                if 'language' not in entry[0]:
                    size = entry[1]['size']
                    url = entry[1]['url']
                    preprocessing = url.split('/')[-2]
                    print(f'corpus: {corpus}, version: {version}, src: {src}, trg: {trg}, documents: {documents}, '
                            f'alignment_pairs: {alignment_pairs}, source_tokens: {source_tokens}, target_tokens: '
                            f'{target_tokens}, latest: {latest}, size: {size}, preprocessing: {preprocessing}, url: {url}')
                    input()

def main():
    URL_BASE = 'https://raw.githubusercontent.com/Helsinki-NLP/OPUS/main/corpus/'
    index_info = read_url(URL_BASE + 'index-info.txt')

    for info in index_info:
        info_s = info.split('/')
        if len(info_s) == 2:
            gen_info = read_url_yaml(URL_BASE + info)
            corpus = gen_info['name']
            latest_v = gen_info['latest release']
            print(corpus, latest_v)
        elif len(info_s) == 3:
            version = info_s[1]
            latest = False
            if version == latest_v:
                latest = True
            stats = info.replace('info', 'statistics')
            corpus_data = read_url_yaml(URL_BASE + stats)
            get_opus_entries(corpus, version, latest, corpus_data)
            get_bitext_entries(corpus, version, latest, corpus_data['bitexts'])
            get_monolingual_entries(corpus, version, latest, corpus_data['monolingual'])
            get_moses_entries(corpus, version, latest, corpus_data['moses'])
            get_tmx_entries(corpus, version, latest, corpus_data['tmx'])

if __name__ == "__main__":
    main()
