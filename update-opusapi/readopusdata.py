import re
import urllib.request
import sqlitetest as st

URL_BASE = "https://object.pouta.csc.fi/OPUS-index"

def read_url(url):
    return urllib.request.urlopen(url).read().decode("utf-8").split("\n")

def get_languages(languages_raw):
    if languages_raw == "README":
        return "", ""
    m = re.search("^(.*?)(\.|$)", languages_raw)
    if m:
        languages = m.group(1).split("-")
    if len(languages) == 2:
        return languages[0], languages[1]
    else:
        return languages[0], ""

st.main()

conn = st.create_connection("opusdata.db")
cur = conn.cursor()

corpora = read_url("{}/{}".format(URL_BASE, "index.txt"))

prev_corpus_name = ""

for line in corpora:
    m = re.search("(.*?)/", line)
    if m:
        corpus_name = m.group(1)
    if corpus_name != prev_corpus_name:
        print("Processing corpus {}".format(corpus_name))
        prev_corpus_name = corpus_name
        releases = read_url("{}/{}/{}".format(URL_BASE, corpus_name, "releases.txt"))
        m = re.search("(.*?)\t", releases[-2])
        if m:
            latest = m.group(1)
            m = re.search("(.*?)\t", releases[-2])
        index = read_url("{}/{}/{}".format(URL_BASE, corpus_name, "index.txt"))
        for item in index:
            m = re.search("^(.*?)/(.*?)/(.*?)(/|$)", item)
            if m:
                languages_raw = m.group(3)
                source, target = get_languages(languages_raw)
                corpus = corpus_name
                preprocessing = m.group(2)
                version = m.group(1)
                url = "https://object.pouta.csc.fi/OPUS-{}/{}".format(corpus_name, item)
                size = ""
                documents = ""
                alignment_pairs = ""
                source_tokens = ""
                target_tokens = ""
                isLatest = version == latest

            opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, isLatest)

            sql = '''INSERT INTO opusfile(source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'''

            cur.execute(sql, opusfile)

conn.commit()
conn.close()
