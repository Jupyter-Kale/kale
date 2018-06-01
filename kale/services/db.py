#!/usr/bin/env python

import sqlite3


class DataStore(object):
    def __init__(self):
        self._conn = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        #self._conn.row_factory = sqlite3.Row
        self._cursor = self._conn.cursor()


class JobStore(DataStore):
    def __init__(self):
        super().__init__()
        try:
            self._cursor.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, name TEXT, qstatus TEXT)")
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def list(self):
        try:
            self._cursor.execute("SELECT * FROM jobs")
            rows = self._cursor.fetchall()
            return rows
        except sqlite3.ProgrammingError as e:
            raise

    def find_by_id(self, id):
        try:
            self._cursor.execute("SELECT * FROM jobs WHERE id=?", (id,))
            row = self._cursor.fetchone()
            return row
        except sqlite3.ProgrammingError as e:
            raise

    def add(self, id, name, qstatus):
        try:
            self._cursor.execute("INSERT INTO jobs VALUES (?,?,?)", (id, name, qstatus))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def remove(self, id):
        try:
            self._cursor.execute("DELETE FROM jobs WHERE id=?", (id,))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def update_status(self, id, qstatus):
        try:
            self._cursor.execute("UPDATE jobs SET qstatus=? WHERE id=?", (qstatus,id))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise


class FileStore(DataStore):
    def __init__(self):
        super().__init__()
        try:
            self._cursor.execute("CREATE TABLE files (file_id INTEGER PRIMARY KEY, job_id INTEGER, name TEXT, position INTEGER)")
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def list(self):
        try:
            self._cursor.execute("SELECT * FROM files")
            rows = self._cursor.fetchall()
            return rows
        except sqlite3.ProgrammingError as e:
            raise

    def find(self, job_id, name):
        try:
            self._cursor.execute("SELECT * FROM files WHERE job_id=? AND name=?", (job_id, name))
            row = self._cursor.fetchone()
            return row
        except sqlite3.ProgrammingError as e:
            raise

    def add(self, job_id, name, position):
        try:
            self._cursor.execute("INSERT INTO files VALUES (NULL,?,?,?)", (job_id, name, position))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def remove(self, job_id, name):
        try:
            self._cursor.execute("DELETE FROM files WHERE job_id=? AND name=?", (job_id, name))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def update_position(self, job_id, name, position):
        try:
            self._cursor.execute("UPDATE files SET position=? WHERE job_id=? AND name=?", (position, job_id, name))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise


class WorkerStore(DataStore):
    def __init__(self):
        super().__init__()
        try:
            self._cursor.execute("CREATE TABLE workers (id TEXT PRIMARY KEY, protocol TEXT, host TEXT, port INTEGER)")
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def list(self):
        try:
            self._cursor.execute("SELECT * FROM workers")
            rows = self._cursor.fetchall()
            return rows
        except sqlite3.ProgrammingError as e:
            raise

    def find(self, worker_id=None):
        try:
            assert worker_id is not None
            self._cursor.execute("SELECT * FROM workers WHERE id=?", (worker_id, ))
            row = self._cursor.fetchone()
            return row
        except sqlite3.ProgrammingError as e:
            raise

    def add(self, worker_id, protocol, host, port):
        try:
            self._cursor.execute("INSERT INTO workers VALUES (?,?,?,?)", (worker_id, protocol, host, port))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def remove(self, worker_id):
        try:
            self._cursor.execute("DELETE FROM workers WHERE id=?", (worker_id, ))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise


class TaskStore(DataStore):
    def __init__(self):
        super().__init__()
        try:
            self._cursor.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY," +
                                 "target BLOB, call TEXT, args BLOB, kwargs BLOB, name TEXT, pid INTEGER)")
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def list(self):
        try:
            self._cursor.execute("SELECT * FROM tasks")
            rows = self._cursor.fetchall()
            return rows
        except sqlite3.ProgrammingError as e:
            raise

    def find(self, task_id=None):
        try:
            assert task_id is not None
            self._cursor.execute("SELECT * FROM tasks WHERE id=?", (task_id, ))
            row = self._cursor.fetchone()
            return row
        except sqlite3.ProgrammingError as e:
            raise

    def add(self, target=None, call=None, args=None, kwargs=None, name=""):
        try:
            self._cursor.execute("INSERT INTO tasks VALUES (NULL,?,?,?,?,?,-1)",
                                 (target, call, args, kwargs, name))
            self._conn.commit()
            self._cursor.execute("SELECT last_insert_rowid()")
            return self._cursor.fetchone()[0]
        except sqlite3.ProgrammingError as e:
            raise

    def update_pid(self, task_id, pid):
        try:
            self._cursor.execute("UPDATE tasks SET pid=? WHERE id=?", (pid, task_id))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise

    def remove(self, task_id):
        try:
            self._cursor.execute("DELETE FROM tasks WHERE id=?", (task_id, ))
            self._conn.commit()
        except sqlite3.ProgrammingError as e:
            raise


if __name__ == "__main__":
    js = JobStore()
    js.add(0, 'test0', 'PD')
    js.add(1, 'test1', 'PD')
    js.add(2, 'test2', 'PD')
    print(js.list())
    print(js.find_by_id(1))
    js.remove(1)
    print(js.find_by_id(1))
    print(js.list())
    js.update_status(0,'R')
    print(js.list())
    print("*"*80)
    fs = FileStore()
    fs.add(0, 'test0', 0)
    fs.add(1, 'test1', 0)
    fs.add(2, 'test2', 0)
    print(fs.list())
    print(fs.find(1, 'test1'))
    fs.remove(1, 'test1')
    print(fs.find(1,'test1'))
    print(fs.list())
    fs.update_position(0,'test0',100)
    print(fs.list())
