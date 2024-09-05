
import sys
import os
import sqlite3
import time

from .utils import convert_seconds

class db:
    def __init__(self, log, init=False):
        # Do we need to do an setup?
        if not os.path.exists(os.path.expanduser('~/.glurm')):
            if not init: raise AssertionError('System not initialized')
            os.mkdir(os.path.expanduser('~/.glurm/'))

        if not os.path.exists(os.path.expanduser('~/.glurm/database.db')):
            if not init: raise AssertionError('System not initialized')
            self.setup()

        # TODO: Test integrity of database.db

        self.con = sqlite3.connect(os.path.expanduser('~/.glurm/database.db'))
        self.cur = self.con.cursor()
        self.log = log

    def setup(self):
        import psutil

        self.con = sqlite3.connect(os.path.expanduser('~/.glurm/database.db'))
        self.cur = self.con.cursor()

        self.cur.execute('CREATE TABLE jobs (jid INT, name TEXT, time_added_to_q TEXT, time_started TEXT, command TEXT, status TEXT, ncpus INT, memory INT, stdout TEXT, stderr TEXT)')
        self.cur.execute('CREATE TABLE finished_jobs (jid INT, time_started TEXT, time_taken TEXT)')

        # Set up the nodes;
        self.cur.execute('CREATE TABLE node_status (nid TEXT PRIMARY KEY, ncpus_total INT, memory_total INT, cpus_allocated INT, mem_allocated INT, status TEXT)')
        svmem = psutil.virtual_memory()
        data = dict(
            ncpus = os.cpu_count(),
            mem = svmem.total,
            nid = 'node001',
            status = 'I',
            ncpus_alloc = 0,
            mem_alloc = 0,
            )

        self.cur.execute('INSERT INTO node_status VALUES (:nid, :ncpus, :mem, :ncpus_alloc, :mem_alloc, :status)', data)

        # we are doing a single node, single computer setup;

        # Set up the persistant info
        self.cur.execute('CREATE TABLE settings (key TEXT, value TEXT)')
        self.cur.execute('INSERT INTO settings VALUES ("cJID", "0")')

        self.con.commit()
        self.con.close()
        self.cur = None

        self.log.info('Set up databases')

    def node_can_accomodate_task(self, ncpus:int=1, maxmem:int=1) -> bool:
        """
        Work out if at least one node is available that can run this task for some list of criteria
        """
        self.cur.execute('SELECT ncpus_total, memory_total FROM node_status')
        nodes = self.cur.fetchall()
        for node in nodes:
            if ncpus <= node[0] and maxmem <= node[1]:
                return True
        return False

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

    def get_jobs_list(self):
        """
        Return all the pertinent details of the jobs.

        Running jobs need to go on top.
        """

        self.cur.execute('SELECT jid, name, time_added_to_q, time_started, status FROM jobs')
        jobs = self.cur.fetchall()

        labeled_jobs = []
        for j in jobs:

            if j[3] != '': #time_started:
                print(j)
                time_queing = '-'
                time_running = convert_seconds(int(time.time() - float(j[2])))
            else:
                time_queing = convert_seconds(int(time.time() - float(j[2])))
                time_running = '-'

            labeled_jobs.append({'jid': j[0],
                'name': j[1],
                'time_queing': time_queing,
                'time_running': time_running,
                'status': j[4],
                })

        return labeled_jobs


    def reserve_next_jid(self):
        self.cur.execute('SELECT value FROM settings WHERE key=?', ('cJID', ))
        res = self.cur.fetchone()

        next_jid = int(res[0])

        self.cur.execute('UPDATE settings SET value=? WHERE key=?', (str(next_jid+1), 'cJID'))
        self.con.commit()

        return next_jid

    def add_task(self, jid, args):
        """

        Add an task to the queue.

        """
        sql_dict = {
            'jid': jid,
            'name': args.job_name,
            'time_added_to_q': str(time.time()),
            'time_started': '',
            'command': str(args.script[0]),
            'status': 'W',
            'ncpus': args.cpus_per_task,
            'memory': args.mem,
            'stdout': args.output,
            'stderr': args.error,
            }

        zipped = []
        for k, v in zip(sql_dict.keys(), sql_dict.values()):
            zipped.append(f':{k}')
        zipped = ', '.join(zipped)

        sql = 'INSERT INTO jobs VALUES ({})'.format(zipped)

        print(sql)

        self.cur.execute(sql, sql_dict)
        self.con.commit()

        # JID INT PRIMARY KEY, name TEXT, time_added_to_q TEXT, time_started TEXT, command TEXT, status TEXT, ncpus INT, memory INT, stdout TEXT, stderr TEXT

    def process_q(self):
        """

        Here's the bg logic controller.

        Suggested to run every ~10s or so.

        """
        pass

