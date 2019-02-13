import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_opusfile(conn, opusfile):
    sql = '''INSERT INTO opusfile(source, target, corpus, preprocessing, version, url, size, latest) VALUES(?,?,?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, opusfile)
    conn.commit()
    return cur.lastrowid

def main():
    sql_create_table = '''CREATE TABLE IF NOT EXISTS opusfile (
    id integer PRIMARY KEY,
    source text,
    target text,
    corpus text,
    preprocessing text,
    version text,
    url text,
    documents integer,
    alignment_pairs integer,
    source_tokens integer,
    target_tokens integer,
    size integer,
    latest text
    );'''
    
    conn = create_connection("opusdata.db")

    with conn:
        create_table(conn, sql_create_table)
        #opusfile = ("en", "fi", "Books", "xml", "latest", "url.com", "1")
        #create_opusfile(conn, opusfile)


if __name__ == '__main__':
    main()


