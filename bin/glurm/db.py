
import sys
import os
import sqlite3

class db:
    def __init__(self, no_init=True):
        # Do we need to do an setup?
        if not os.path.exists(os.path.expanduser('~/.glurm')):
            if no_init: raise AssertionError('System not initialized')
            os.mkdir(os.path.expanduser('~/.glurm/'))

        if not os.path.exists(os.path.expanduser('~/.glurm/database.db')):
            if no_init: raise AssertionError('System not initialized')
            self.setup()

        # TODO: Test integrity of database.db

        self.con = sqlite3.connect(os.path.expanduser('~/.glurm/database.db'))
        self.cur = self.con.cursor()

    def setup(self):
        import psutil

        self.con = sqlite3.connect(os.path.expanduser('~/.glurm/database.db'))
        self.cur = self.con.cursor()

        self.cur.execute('CREATE TABLE jobs (JID INT PRIMARY KEY, name TEXT, time_added_to_q TEXT, time_started TEXT, command TEXT, status TEXT, ncpus INT, memory INT, stdout TEXT, stderr TEXT)')
        self.cur.execute('CREATE TABLE finished_jobs (JID INT PRIMARY KEY, time_started TEXT, time_taken TEXT)')

        # Set up the nodes;
        self.cur.execute('CREATE TABLE node_status (NID TEXT PRIMARY KEY, ncpus_total INT, memory_total INT, cpus_allocated INT, mem_allocated INT, status TEXT)')
        svmem = psutil.virtual_memory()
        data = dict(
            ncpus = os.cpu_count(),
            mem = svmem.total,
            NID = 'node001',
            status = 'I',
            ncpus_alloc = 0,
            mem_alloc = 0,
            )

        self.cur.execute('INSERT INTO node_status VALUES (:NID, :ncpus, :mem, :ncpus_alloc, :mem_alloc, :status)', data)

        # we are doing a single node, single computer setup;

        # Set up the persistant info
        self.cur.execute('CREATE TABLE settings (key TEXT, value TEXT)')
        self.cur.execute('INSERT INTO settings VALUES ("cJID", "0")')

        self.con.commit()
        self.con.close()
        self.cur = None

    def commit(self):
        self.con.commit()

    def get_handlers(self):
        # Ideally all changes should be done here;
        return con, cur

    def get_node_data(self):
        """
        Return the node details
        """
        self.cur.execute('SELECT * FROM node_status')

        res = self.cur.fetchall()

        nodes = []
        for item in res:
            # pack nicely;
            node = dict(
                NID = item[0],
                ncpus = item[1],
                mem = f'{item[2] //1024:,}M',
                ncpus_alloc = item[3],
                mem_alloc = f'{item[4]//1024:,}',
                status = item[5],
                )
            nodes.append(node)
        return nodes

    def reserve_next_jid(self):
        self.cur.execute('SELECT value FROM settings WHERE key=?', ('cJID', ))
        res = self.cur.fetchone()

        next_jid = int(res[0])

        self.cur.execute('UPDATE settings SET value=? WHERE key=?', (str(next_jid+1), 'cJID'))
        self.con.commit()

        return next_jid
