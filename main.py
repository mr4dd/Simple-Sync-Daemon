from sys import argv
import sqlite3
import os
import hashlib
import time
import base64
from watchdog.observers import Observer 
from watchdog.events import FileSystemEventHandler
import paramiko

file_list = []
HOME = os.getenv("HOME")
db = os.path.join(HOME, '.local', 'sync.db')

con = sqlite3.connect(db)
cur = con.cursor()

class Watcher(FileSystemEventHandler):
    def __init__(self, process_callback):
        self.process_callback = process_callback

    def on_modified(self, event):
        if not event.is_directory:
            if event.src_path is not db:
                self.process_callback(event.src_path)

    def on_created(self, event):
        if not event.is_directory:
            self.process_callback(event.src_path)

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

def handle_file(file):
    try:
        log(1, f"attempting to process file: {file}")
        res, mtime = process_file(file)

        rows = cur.execute(f"SELECT file_hash, path FROM files WHERE file_hash=?", (res))
        if rows.fetchone() is None:
            cur.execute(f"INSERT INTO files(file_hash, path, date) VALUES(?, ?, ?)", (res, file, mtime))
        else:
            cur.execute(f"UPDATE files SET file_hash=?, date=? WHERE path=?", (res, file, mtime))
        # handle server file verification and syncing later cause my neck HURTS from staring at my second monitor for 3 hours
    except Exception as e:
        log(3, f"processing file: {file} failed {e}")

class RemoteHandler():
    def __init__(self, client):
        self.client = client

    def process(self, file):
        with self.client.open(remote_path, "rb") as f:
            data = f.read()
            digest = hashlib.md5(data).digest()
            return base64.b64encode(digest)

    def recurse(self, remote_path):
        for entry in self.client.listdir_attr(remote_path):
            full_path = f"{remote_path}/{entry.filename}"
            mode = entry.st_mode

            if stat.S_ISDIR(mode):
                yield from first_index_remote(client, full_path)
            elif stat.S_ISREG(mode):
                yield full_path
    
    def index(self, path):
        for file in self.recurse(self.client.normalize(path)):
            yield self.process(file), file

def index_remote(handler):
    cur.execute("BEGIN")
    for data, path in handler.index("files"):
        cur.execute("INSERT INTO remote(file_hash, path, date) VALUES(?, ?, ?)", (data, file, int(time.time())))
    cur.execute("COMMIT")

def main(paths: str, client):
    handler = RemoteHandler(client)
    if (not verify_index()):
        log(1, "no index found, starting indexer")
        first_index(paths)
        index_remote(handler)

    log(1, "monitoring filesystem for changes")
    watch = Watcher(process_callback=handle_file)

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

    transport = paramiko.Transport((argv[1], 22))
    #TODO: get credentials and server from environment variables instead
    transport.connect(username="placeholder", password="placeholder")
    client = paramiko.SFTPClient.from_transport(transport)

    main(argv[2:], client)
