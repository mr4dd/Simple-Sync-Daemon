from sys import argv
import sqlite3
import os
import hashlib
import time
import base64

file_list = []
HOME = os.getenv("HOME")
db = os.path.join(HOME, '.local', 'sync.db')

con = sqlite3.connect(db)
cur = con.cursor()


def log(ltype, message):
    typem = {1: '[INF]', 2: '[WARN]', 3: '[ERR]'}
    print(f"{argv[0]} {time.time()} {typem[ltype]}: {message}")

def verify_index():
    rows = cur.execute('SELECT date FROM files LIMIT 1')
    if rows.fetchone() is None:
        return False
    else:
        return True

def process_file(file):
    size = os.path.getsize(file)
    res = ''
    # ignoring big files for speed's sake, ideally i should be reading the file in chunks and hashing
    # incrementally but i just want this to work
    if (size < 100 *1024^2):
        with open(file, 'rb') as fd:
            content = fd.read()
            hash_digest = hashlib.md5(content).digest()
            res = base64.b64encode(hash_digest)
    modify_time = os.path.getmtime(file)

    return res, modify_time

def first_index(paths):
        cur.execute("BEGIN")
        try:
            start = time.time()
            file_count = 0
            for path in paths:
                for (root, dirs, files) in os.walk(path):
                    for file in files:
                        file = os.path.join(root,file)
                        try:
                            res, mtime = process_file(file)
                            cur.execute("INSERT INTO files(file_hash, path, date) VALUES(?, ?, ?)",
                                        (res, file, mtime)
                            )
                            file_count += 1
                        except Exception as e:
                            log(3, "file processing failed for " + file + " " + str(e))
                            continue

                cur.execute("COMMIT")
            end = time.time()
            log(1, f"indexed {file_count} files in {end - start} seconds")
        except TypeError as e:
            print(e)
            cur.execute("ROLLBACK")
def main(paths: str):

    if (not verify_index()):
        log(1, "no index found, starting indexer")
        first_index(paths)
    log(1, "monitoring filesystem for changes")


if __name__ == '__main__':
    if (len(argv) <= 1):
        print("please provide a directory/list of directories to scan")
        exit()
    if not os.path.isfile(db):
        log(2, "no DB detected, creating one now")
        cur.execute("""CREATE TABLE IF NOT EXISTS files(
        idx INTEGER PRIMARY KEY AUTOINCREMENT,
        file_hash TEXT,
        path TEXT,
        date INTEGER)""")

    main(argv[1:])
