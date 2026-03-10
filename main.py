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
from functools import partial
from threading import Timer

HOME = os.getenv("HOME")
db = os.path.join(HOME, '.local', 'sync.db')
logfile = os.path.join(HOME, '.simplesync.log')

load_dotenv()
remote_dir = os.getenv("REMOTEDIR")

cur = con = None

MAX_FILE_SIZE=100*1024**2

class ConnectionManager():
    def __init__(self, sftp_server: tuple, username: str, password: str):
        self.sftp_server = sftp_server
        self.username = username
        self.password = password
        self.transport = None
        self.client = self._create_connection()

    def _create_connection(self):
        self.transport = paramiko.Transport(self.sftp_server)
        self.transport.set_keepalive(30)
        self.transport.connect(username=self.username, password=self.password)
        client = paramiko.SFTPClient.from_transport(self.transport)
        return client

    def _check_session(self):
        if not self.transport.is_active() or self.transport is None:
            self.transport.close()
            self.client = self._create_connection()

    def upload(self, local: str, remote: str):
        self._check_session()
        try:
            self.client.put(local, remote)
        except Exception as e:
            log(3, e)

    def read_file(self, file: str)->bytes:
        self._check_session()
        with self.client.open(file, "rb") as fd:
            return fd.read()

    def list_dirattr(self, remote_path: str) -> list[str]:
        self._check_session()
        return self.client.listdir_attr(remote_path)

    def normalize(self, path: str)-> str:
        self._check_session()
        return self.client.normalize(path)
    
    def stat(self, path: str)->str:
        self._check_session()
        return self.client.stat(path)
    
    def mkdir(self, path: str):
        self._check_session()
        self.client.mkdir(path)

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
    def __init__(self, process_callback, delay=0.5):
        self.process_callback = process_callback
        self.delay = delay
        self._timer = None

    def debounce_event(self, event):
        if self._timer:
            self._timer.cancel()
        self._timer = Timer(self.delay, self.process_callback, [event.src_path])
        self._timer.start()

    def on_modified(self, event):
        if not event.is_directory:
            if event.src_path is not db:
                self.debounce_event(event)
    def on_created(self, event):
        if not event.is_directory:
            if event.src_path is not db:
                self.debounce_event(event)

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
                res = base64.b64encode(hash_digest).decode('utf-8')
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

def log(ltype, message):
    typem = {1: '[INF]', 2: '[WARN]', 3: '[ERR]'}
    message = f"{argv[0]} {time.strftime('%X %x')} {typem[ltype]}: {message}"
    print(message)
    with open(logfile, 'a') as lfd:
        lfd.write(message+'\n')

def verify_index():
    rows = cur.execute('SELECT date FROM files LIMIT 1')
    if rows.fetchone() is None:
        return False
    else:
        return True

class RemoteHandler():
    def __init__(self, connectionManager):
        self.connectionManager = connectionManager

    def process(self, file):
        try:
            data = connectionManager.read_file(file)
            digest = hashlib.sha256(data).digest()
            return base64.b64encode(digest)
        except Exception as e:
            log(3, f"an exception occured while trying to process {file}. {e}")
            return -1

    def recurse(self, remote_path):
        for entry in self.connectionManager.list_dirattr(remote_path):
            full_path = os.path.join(remote_path, entry.filename)
            mode = entry.st_mode

            if stat.S_ISDIR(mode):
                yield from self.recurse(full_path)
            elif stat.S_ISREG(mode) and entry.st_size < MAX_FILE_SIZE:
                yield full_path
    
    def index(self, path):
        for file in self.recurse(self.connectionManager.normalize(path)):
            yield self.process(file), file

    def ensure_dir_exists(self, path: str)->bool:
        try:
            self.connectionManager.stat(path)
            return True
        except FileNotFoundError:
            return False

    def rmkdir(self, path: str):
        """
            recursive mkdir on the remote
        """
        subdirs = path.split(os.sep)
        builddir = ''
        for sub in subdirs:
            builddir = os.path.join(builddir,sub)
            if self.ensure_dir_exists(builddir) == False:
                try:
                    self.connectionManager.mkdir(builddir)
                except PermissionError:
                    raise PermissionError(f"could not create directory {builddir}")

def handle_file(handler: RemoteHandler, fm: FileManager, file: str):
    try:
        with sqlite3.connect(db) as conn:
            cur = conn.cursor()
            log(1, f"attempting to process file: {file}")
            res, mtime = LocalHandler().process_file(file)
            if res == '':
                return
            rows = cur.execute(f"SELECT file_hash, path FROM files WHERE file_hash=?", (res,))
            if rows.fetchone() is None:
                cur.execute(f"INSERT INTO files(file_hash, path, date) VALUES(?, ?, ?)", (res, file, mtime))
            else:
                cur.execute(f"UPDATE files SET file_hash=?, date=? WHERE path=?", (res, mtime, file))
        sync_files(handler, fm, [(res, file)])

    except Exception as e:
        log(3, f"processing file: {file} failed {e}")

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

def find_diff()->list:
    query_diff_hash = """
        SELECT f.file_hash, f.path FROM files f LEFT JOIN remote r ON f.file_hash = r.file_hash WHERE r.path IS NULL
    """
    rows = cur.execute(query_diff_hash)
    results = rows.fetchall()
    if results is None:
        return []
    else:
        file_paths: list = []
        for result in results:
            file_paths.append((result[0], result[1]))
        
        return file_paths

def sync_files(handler: RemoteHandler, fm: FileManager, files_to_be_synced: list[str]):
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        for f in files_to_be_synced:
            try:
                file = fm.map_local_to_remote(f[1])
                remote_directory = os.path.dirname(file)
                exists = handler.ensure_dir_exists(remote_directory)
                if exists == False:
                    log(1, f"creating remote directory {remote_dir}")
                    handler.rmkdir(remote_directory)
                try:
                    log(1, f"trying to upload file {file}")
                    handler.connectionManager.upload(f[1], file)
                    query = """
                    INSERT INTO remote(file_hash, path, date) VALUES(?, ?, ?)
                    """
                    cur.execute(query, (f[0], file, int(time.time())))
                    conn.commit()
                except Exception as e:
                    log(2, f"file {f[1]} could not be uploaded: {e}")
                    conn.rollback()
            except ValueError as e:
                log(2, f"{f} failed to make remote path: {e}")

def getCreds():
    username = os.getenv("SYNCUSR")
    password = os.getenv("SYNCPWD")
    host = os.getenv("REMOTE")
    port = os.getenv("PORT")
    
    if (username is None or password is None or host is None or port is None):
        print("please supply all the required environment variables")
        exit()
    
    return (host, int(port)), username, password


def main(paths: list[str]):
    sftp_server, username, password = getCreds()
    connectionManager = ConnectionManager(sftp_server, username, password)
    handler = RemoteHandler(connectionManager)

    if (not verify_index()):
        log(1, "no index found, starting indexer")
        LocalHandler().first_index(paths)
        index_remote(handler)
    
    # syncing the files that only exist on the client to the server
    # before monitoring changes
    files_to_be_synced = find_diff()
    con.close()

    fm = FileManager("config.json")
    sync_files(handler, fm, files_to_be_synced)

    log(1, "monitoring filesystem for changes")
    handler = partial(handle_file, handler, fm)
    watch = Watcher(process_callback=handler)
    observer = Observer()
    for path in paths:
        observer.schedule(watch, path, recursive=True)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log(1, "Keyboard interrupt recieved, exiting...")
    except Exception as e:
        log(3, f"Unknown exception occurred: {e}")
    finally:
        observer.stop()
        observer.join()


if __name__ == '__main__':
    
    if (len(argv) <= 1):
        print("please provide a directory/list of directories to scan.")
        print("sync [DIR]...")
        exit()
    
    if not os.path.isfile(db):
        con = sqlite3.connect(db, check_same_thread=False)
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
        con = sqlite3.connect(db, check_same_thread=False)
        cur = con.cursor()

    main(argv[1:])
