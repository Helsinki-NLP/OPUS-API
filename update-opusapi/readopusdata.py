import re
import sqlitetest as st

st.main()

f = open("opusdata.txt")

errorlog = open("opusdata_errors.log", "w")
errornum = 0

url = ""

conn = st.create_connection("opusdata.db")
cur = conn.cursor()

latest = ""

oid=0
for line in f:
    '''
for i in range(500):
    line = f.readline()
    #print(line)
    #'''
    oid += 1

    origLine = line
    line = line.strip()
    line = line.split(" ")
    if line[0] == "" or line[0] == "total":
#        print("")
        continue
    if line[0][-1] == ":":
        url = line[0][:-1]+"/"

    if len(line) >= 2 and line[0][-1] != ":" and line[-1][-1] != "/":
        #print(url, end=" ")
        if " latest -> " in origLine:
            latest = line[-1]

        m = re.search("/proj/nlpl/data/OPUS/(.*?)/(.*?)/(.*?)/(.*?)$", url+line[-1])
        if m:
            corpus = m.group(1)
            version = m.group(2)
            preprocessing = m.group(3)
            if preprocessing == "info":
                continue
            rest = m.group(4)
            if "Makefile" in rest:
                continue
            #print(corpus, version, preprocessing, rest)
                
            if version == latest:
                isLatest = "True"
            else:
                isLatest = "False"

            sourceandtarget = False

            altstr = ""
            if ".alt.xml.gz" in rest:
                altstr = ".alt"

            if rest.startswith(corpus):
                n = re.search("\.(.*?)\.gz", rest)
                if n:
                    source = n.group(1).replace("raw.", "")
                    target = ""
                    infopath = "/proj/nlpl/data/OPUS/{0}/{1}/info/{2}{3}.info".format(corpus, version, source, altstr)
            elif "-" not in rest:
                n = re.search("^(.*?)\.", rest)
                if n:
                    source = n.group(1)
                    target = ""
                    infopath = "/proj/nlpl/data/OPUS/{0}/{1}/info/{2}{3}.info".format(corpus, version, source, altstr)
            else:
                n = re.search("(.*?)\-(.*?)[\.|/]", rest)
                if n:
                    source = n.group(1)
                    target = n.group(2)
                    infopath = "/proj/nlpl/data/OPUS/{0}/{1}/info/{2}-{3}{4}.info".format(corpus, version, source, target, altstr)
                    sourceandtarget = True

            numbers = ["","","",""]

            try:
                with open(infopath) as infof:
                    numbers = infof.readlines()
            except Exception as e:
                errorlog.write(str(e) + "\n")
                errornum += 1

            #print(corpus, version, preprocessing, source, target)
            link = url+line[-1]
            link = link.replace("/proj/nlpl/data/OPUS/", "")


            #print(opusfile)

            if sourceandtarget:
                if len(numbers) < 4:
                    #print(link)
                    numbers = ["","","",""]
                opusfile = (source, target, corpus, preprocessing, version, link, line[0], numbers[0].strip(), numbers[1].strip(), numbers[2].strip(), numbers[3].strip(), isLatest)
                sql = '''INSERT INTO opusfile(source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, target_tokens, latest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'''
            else:
                opusfile = (source, target, corpus, preprocessing, version, link, line[0], numbers[0].strip(), numbers[1].strip(), numbers[2].strip(), isLatest)
                sql = '''INSERT INTO opusfile(source, target, corpus, preprocessing, version, url, size, documents, alignment_pairs, source_tokens, latest) VALUES(?,?,?,?,?,?,?,?,?,?,?)'''
                

            cur.execute(sql, opusfile)
                
        else:
            pass
            #print("")

    corpus = ""
    version = ""
    preprocessing = ""
    source = ""
    target = ""

    if oid % 100 == 0:
        print(oid, "lines processed,", errornum, "errors encountered", end="\r")

print(oid, "lines processed,", errornum, "errors encountered", end="\r")
print("\nopusdata.db created! Check 'opusdata_error.log' for errors.\n")
errorlog.close()
f.close()
conn.commit()
conn.close()
