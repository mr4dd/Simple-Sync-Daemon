from sys import argv
import sqlite3
import os
import hashlib
import time
import base64
import stat
from watchdog.observers import Observer 
from watchdog.events import FileSystemEventHandler
import paramiko
from dotenv import load_dotenv
import json


file_list = []

HOME = os.getenv("HOME")
db = os.path.join(HOME, '.local', 'sync.db')

load_dotenv()
remote_dir = os.getenv("REMOTEDIR")

cur = con = None

MAX_FILE_SIZE=100*1024**2

class FileManager():
    def __init__(self, config_file):
        self.mappings = self._load_mapping(config_file)
        self.extensions = self._load_extensions(config_file)

    def _load_mapping(self, config_file: str) -> dict:
        with open(config_file, 'r') as cfd:
            return json.load(cfd)["mappings"]

    def _load_extensions(self, config_file: str) -> dict:
        with open(config_file, 'r') as cfd:
            return json.load(cfd)["extensions"]

    def map_local_to_remote(self, path: str) -> str:
        extension = path.split(".")[-1].lower()
        filetype = ''
        if extension in self.extensions["media"]:
            filetype = "media"
        elif extension in self.extensions["docs"]:
            filetype = "docs"
        elif extension in self.extensions["audio"]:
            filetype = "audio"
        else:
            raise ValueError("extension unrecognized")
      
        return self.create_remote_path(filetype, path)

    def _strip_base(self, path: str) -> str:
        new_path = os.path.abspath(os.path.expanduser(path))
        home = os.path.expanduser("~")

        if new_path.startswith(home + os.sep):
            relative_path = new_path[len(home)+1:]
        else:
            return new_path
        
        parts = relative_path.split(os.sep, 1)
        if len(parts) == 2:
            return parts[1] 
        else:
            return parts[0]

    def create_remote_path(self, dirtype: str, path: str)->str:
        remote_base = self.mappings[dirtype]
        local_stripped = self._strip_base(path)
        
        # if there is an OS mismatch between the remote and the host this might cause problems
        remote = os.path.join(remote_base, local_stripped)
        return remote

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

class LocalHandler():
    def process_file(self, file):
        size = os.path.getsize(file)
        res = ''
        # ignoring big files for speed's sake, ideally i should be reading the file in chunks and hashing
        # incrementally but i just want this to work
        if (size < MAX_FILE_SIZE):
            with open(file, 'rb') as fd:
                content = fd.read()
                hash_digest = hashlib.sha256(content).digest()
                res = base64.b64encode(hash_digest)
        modify_time = os.path.getmtime(file)

        return res, modify_time

    def first_index(self, paths):
        cur.execute("BEGIN")
        try:
            start = time.time()
            file_count = 0
            for path in paths:
                for (root, dirs, files) in os.walk(path):
                    for file in files:
                        file = os.path.join(root,file)
                        try:
                            res, mtime = self.process_file(file)
                            if res == '':
                                continue
                            cur.execute("INSERT INTO files(file_hash, path, date) VALUES(?, ?, ?)",
                                        (res, file, mtime)
                            )
                            file_count += 1
                        except Exception as e:
                            log(3, f"file processing failed for {file}: {e}")
                            continue

                cur.execute("COMMIT")
            end = time.time()
            log(1, f"indexed {file_count} files in {end - start} seconds")
        
        except TypeError as e:
            log(3, str(e))
            cur.execute("ROLLBACK")

def handle_file(file):
    try:
        log(1, f"attempting to process file: {file}")
        res, mtime = process_file(file)
        if res == '':
            return
        rows = cur.execute(f"SELECT file_hash, path FROM files WHERE file_hash=?", (res))
        if rows.fetchone() is None:
            cur.execute(f"INSERT INTO files(file_hash, path, date) VALUES(?, ?, ?)", (res, file, mtime))
        else:
            cur.execute(f"UPDATE files SET file_hash=?, date=? WHERE path=?", (res, mtime, file))

    except Exception as e:
        log(3, f"processing file: {file} failed {e}")

class RemoteHandler():
    def __init__(self, client):
        self.client = client

    def process(self, file):
        try:
            with self.client.open(file, "rb") as f:
                data = f.read()
                digest = hashlib.sha256(data).digest()
                return base64.b64encode(digest)
        except Exception as e:
            log(3, f"an exception occured while trying to process {file}. {e}")
            return -1

    def recurse(self, remote_path):
        for entry in self.client.listdir_attr(remote_path):
            full_path = os.path.join(remote_path, entry.filename)
            mode = entry.st_mode

            if stat.S_ISDIR(mode):
                yield from self.recurse(full_path)
            elif stat.S_ISREG(mode) and entry.st_size < MAX_FILE_SIZE:
                yield full_path
    
    def index(self, path):
        for file in self.recurse(self.client.normalize(path)):
            yield self.process(file), file

def index_remote(handler):
    files = 0;
    timebefore = time.time()

    log(1, "indexing remote files, this may take a while")
    
    cur.execute("BEGIN")
    for data, path in handler.index(remote_dir):
        if data == -1:
            continue
        cur.execute("INSERT INTO remote(file_hash, path, date) VALUES(?, ?, ?)", (data, path, int(time.time())))
        files += 1
    cur.execute("COMMIT")

    timeafter = time.time()
    log(1, f"indexed {files} files in {timeafter - timebefore} seconds")

def main(paths: str, client):
    handler = RemoteHandler(client)
    if (not verify_index()):
        log(1, "no index found, starting indexer")
        LocalHandler().first_index(paths)
        index_remote(handler)
    
    # syncing the files that only exist on the client to the server
    # before monitoring changes
    
    log(1, "monitoring filesystem for changes")
    watch = Watcher(process_callback=handle_file)

if __name__ == '__main__':
    
    if (len(argv) <= 1):
        print("please provide a directory/list of directories to scan.")
        print("sync [DIR]...")
        exit()
    
    if not os.path.isfile(db):
        con = sqlite3.connect(db)
        cur = con.cursor()
        log(2, "no DB detected, creating one now")

        cur.execute("""CREATE TABLE IF NOT EXISTS files(
        idx INTEGER PRIMARY KEY AUTOINCREMENT,
        file_hash TEXT,
        path TEXT,
        date INTEGER)""")

        cur.execute("""CREATE TABLE IF NOT EXISTS remote(
        idx INTEGER PRIMARY KEY AUTOINCREMENT,
        file_hash TEXT,
        path TEXT,
        date INTEGER)""")
    else:
        con = sqlite3.connect(db)
        cur = con.cursor()

    username = os.getenv("SYNCUSR")
    password = os.getenv("SYNCPWD")
    host = os.getenv("REMOTE")
    port = os.getenv("PORT")
    
    if (username is None or password is None or host is None or port is None):
        print("please supply all the required environment variables")
        exit()

    transport = paramiko.Transport((host, int(port)))
    transport.set_keepalive(30)
    transport.connect(username=username, password=password)
    client = paramiko.SFTPClient.from_transport(transport)

    main(argv[1:], client)
