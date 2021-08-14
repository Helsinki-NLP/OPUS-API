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

def get_latest_version(corpus_name):
    latest = ""
    releases = read_url("{}/{}/{}".format(URL_BASE, corpus_name, "releases.txt"))
    m = re.search("(.*?)\t", releases[-2])
    if m:
        latest = m.group(1)
    return latest

def get_info_dict(corpus_name):
    info_dict = {}
    infos = read_url("{}/{}/{}".format(URL_BASE, corpus_name, "info.txt"))
    for item in infos:
        m = re.search("info/(.*?)\.info:(.*?)$", item)
        if m:
            if m.group(1) not in info_dict.keys():
                info_dict[m.group(1)] = []
            info_dict[m.group(1)].append(int(m.group(2)))
    return info_dict

def get_item_info(src, trg, prepro, info_dict, corpus_name):
    if prepro == "moses":
        prepro = "txt"
    r_i = ["","","",""]
    try:
        if trg != "":
            r_i = info_dict["{}-{}".format(src, trg)]
            if prepro in ("tmx", "moses"):
                i = info_dict["{}-{}.{}".format(src, trg, prepro)]
                r_i[1] = i[0]
                r_i[2] = i[1]
                r_i[3] = i[2]
        elif src not in ("", "README"):
            r_i = info_dict[src] + [""]
    except Exception as e:
        error_log.write("Info not found for \"{} {} {} {}\"\n".format(corpus_name, src, trg, prepro))
        return "","","",""
    return r_i[0], r_i[1], r_i[2], r_i[3]

st.main()

conn = st.create_connection("opusdata.db")
cur = conn.cursor()

corpora = read_url("{}/{}".format(URL_BASE, "index.txt"))

prev_corpus_name = ""

error_log = open("error.log", "w")

for line in corpora:
    m = re.search("(.*?)/", line)
    if m:
        corpus_name = m.group(1)
    if corpus_name != prev_corpus_name:
        print("Processing corpus {}".format(corpus_name))
        prev_corpus_name = corpus_name
        latest = get_latest_version(corpus_name)
        info_dict = get_info_dict(corpus_name)
        # index = read_url("{}/{}/{}".format(URL_BASE, corpus_name, "index-long.txt"))
        index = read_url("{}/{}/{}".format(URL_BASE, corpus_name, "index-filesize.txt"))
        for item in index:
            # m = re.search("^\s*(.*?) .*? .*? (.*?)$", item)
            m = re.search("^\s*(.*?) (.*?)$", item)
            if m:
                item_path = m.group(2)
                m2 = re.search("^(.*?)/(.*?)/(.*?)(/|$)", item_path)
                if m2:
                    languages_raw = m2.group(3)
                    source, target = get_languages(languages_raw)
                    corpus = corpus_name
                    preprocessing = m2.group(2)
                    version = m2.group(1)
                    # url = "https://object.pouta.csc.fi/OPUS-{}/{}".format(corpus_name, item_path)
                    url = "{}/{}".format(corpus_name, item_path)
                    # size = m.group(1)
                    size = int(int(m.group(1))/1000)
                    documents, alignment_pairs, source_tokens, target_tokens = get_item_info(source, target, preprocessing, info_dict, corpus_name)
                    if version == latest:
                        isLatest = 'True'
                    else:
                        isLatest = 'False'
                        # isLatest = version == latest

                    opusfile = (source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, isLatest)

                    sql = '''INSERT INTO opusfile(source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'''

                    cur.execute(sql, opusfile)

conn.commit()
conn.close()
error_log.close()
