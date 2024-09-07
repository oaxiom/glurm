
import sys
import os
import sqlite3
import time
import subprocess
import random
import stat

from .utils import convert_seconds

class db:
    def __init__(self, log, init=False):
        self.log = log
    
        # Do we need to do an setup?
        if not os.path.exists(os.path.expanduser('~/.glurm')):
            if not init: raise AssertionError('System not initialized')
            os.mkdir(os.path.expanduser('~/.glurm/'))

        if not os.path.exists(os.path.expanduser('~/.glurm/database.db')):
            if not init: raise AssertionError('System not initialized')
            self.setup()

        # TODO: Test integrity of database.db

        self.con = sqlite3.connect(os.path.expanduser('~/.glurm/database.db'))
        self.con.row_factory = sqlite3.Row # Return as dicts;
        self.cur = self.con.cursor()

    def setup(self):
        self.con = sqlite3.connect(os.path.expanduser('~/.glurm/database.db'))
        self.con.row_factory = sqlite3.Row # Return as dicts;
        self.cur = self.con.cursor()

        # This table gets a bit complicated:

        jobs_table = '''
        CREATE TABLE jobs (
            jid INT,
            pid INT,
            cwd TEXT,
            script TEXT,
            name TEXT,
            time_added_to_q TEXT,
            time_started TEXT,
            command TEXT,
            status TEXT,
            ncpus INT,
            memory INT,
            stdout TEXT,
            stderr TEXT,
            tmp_filename TEXT
            )
        '''

        self.cur.execute(jobs_table)

        # TODO: Sort this out to store relevant data
        self.cur.execute('CREATE TABLE finished_jobs (jid INT, time_started TEXT, time_taken TEXT)')

        # Set up the nodes;
        # We are doing a single node, single computer setup;
        # TODO: Put this into a conf file.
        node_table = '''
            CREATE TABLE node_status (
                nid TEXT PRIMARY KEY, 
                ncpus_total INT, 
                memory_total INT, 
                cpus_allocated INT, 
                mem_allocated INT, 
                status TEXT
                )
        '''
        self.cur.execute(node_table)
        #svmem = psutil.virtual_memory()
        data = dict(
            ncpus = os.cpu_count(),
            mem = 8000000000, # Surpsingly no easy way to do this...
            nid = 'node001',
            status = 'I',
            ncpus_alloc = 0,
            mem_alloc = 0,
            )

        self.cur.execute('INSERT INTO node_status VALUES (:nid, :ncpus, :mem, :ncpus_alloc, :mem_alloc, :status)', data)

        # Set up the persistant info for settings, stats, etc.
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
        Return the node details. This is for sinfo. There is another function that packs the 
        data for internal use.
        """
        self.cur.execute('SELECT * FROM node_status')
        res = self.cur.fetchall()
        nodes = [dict(node) for node in res]
        return nodes

    def get_jobs_list(self, running_only=False, waiting_only=False):
        """
        Return all the pertinent details of the jobs.

        Running jobs need to go on top.
        """
        assert not (waiting_only and running_only), "Can't have both running_only and waiting_only"

        if running_only:
            self.cur.execute('SELECT * FROM jobs WHERE status="R"')
        elif waiting_only:
            self.cur.execute('SELECT * FROM jobs WHERE status="W"')
        else:
            self.cur.execute('SELECT * FROM jobs')

        jobs = self.cur.fetchall()

        if not jobs:
            return None

        labeled_jobs = []
        for j in jobs:
            j = dict(j)

            if j['time_started'] != '': #time_started:
                time_queing = '-'
                time_running = convert_seconds(int(time.time() - float(j['time_started'])))
            else:
                time_queing = convert_seconds(int(time.time() - float(j['time_added_to_q'])))
                time_running = '-'

            j['time_queing'] = time_queing
            j['time_running'] = time_running
            
            labeled_jobs.append(j)

        # TODO: sort by time on q:

        return labeled_jobs

    def get_node_states(self, idle_or_mixed_only=True):
        """
        Return node details to enable making a choice 
        """
        if idle_or_mixed_only:
            self.cur.execute('SELECT * FROM node_status WHERE status="I"')
            idle_nodes = self.cur.fetchall()
            self.cur.execute('SELECT * FROM node_status WHERE status="M"')
            mixed_nodes = self.cur.fetchall()
            nodes = idle_nodes + mixed_nodes
        else:
            self.cur.execute('SELECT * FROM node_status')
            nodes = self.cur.fetchall()

        # pack the results:
        labelled_nodes = []
        for node in nodes:
            anode = dict(node)
            
            anode['ncpus_avail'] = node['ncpus_total'] - node['cpus_allocated'],
            anode['mem_avail'] = node['memory_total'] - node['mem_allocated'],
            
            labelled_nodes.append(anode)

        return labelled_nodes        

    def reserve_next_jid(self):
        self.cur.execute('SELECT value FROM settings WHERE key=?', ('cJID', ))
        res = self.cur.fetchone()

        next_jid = int(res[0])

        self.cur.execute('UPDATE settings SET value=? WHERE key=?', (str(next_jid+1), 'cJID'))
        self.con.commit()

        return next_jid

    def add_task(self, jid, args, pwd):
        """

        Add an task to the queue.

        """

        script = None
        with open(args.script[0], 'rt') as oh:
            script = oh.read()

        sql_dict = {
            'jid': jid,
            'pid': 0,
            'cwd': pwd,
            'script': script, # 2Gb limit
            'name': args.job_name,
            'time_added_to_q': str(time.time()),
            'time_started': '',
            'command': str(args.script[0]),
            'status': 'W',
            'ncpus': args.cpus_per_task,
            'memory': args.mem,
            'stdout': args.output,
            'stderr': args.error,
            'tmp_filename': '',
            }

        zipped = []
        for k, v in zip(sql_dict.keys(), sql_dict.values()):
            zipped.append(f':{k}')
        zipped = ', '.join(zipped)

        sql = 'INSERT INTO jobs VALUES ({})'.format(zipped)

        self.cur.execute(sql, sql_dict)
        self.con.commit()

        # JID INT PRIMARY KEY, name TEXT, time_added_to_q TEXT, time_started TEXT, command TEXT, status TEXT, ncpus INT, memory INT, stdout TEXT, stderr TEXT

    def allocate_job_to_node(self, job, node):
        """
        Allocate the job to a node, set all statuses in the DB
        """
        
        # Reserve the node and set the job to running
        # Does it take all of the node, or some of the node?
        # TODO: Should be doen in one command to reduce race conditions;
        print(job)
        self.cur.execute('UPDATE node_status SET status="M", cpus_allocated=? WHERE nid=?', (job['ncpus'], node['nid']))
        self.cur.execute('UPDATE jobs SET status="R" WHERE jid=?', (job['jid'],))
        self.con.commit()
        
        # See if the node is now full
        self.cur.execute('SELECT cpus_allocated, ncpus_total FROM node_status WHERE nid=?', (node['nid'], ))
        res = self.cur.fetchone()
        if res['ncpus_total'] >= res['cpus_allocated']:
            self.cur.execute('UPDATE node_status SET status="A" WHERE nid=?', (node['nid'], ))
            self.con.commit()
        
        # Run the job, get a PID and update the DB
        
        tmp_filename = f'/tmp/glurm.{time.time():.0f}.{job["jid"]}.{node["nid"]}.{job["command"]}'
        with open(tmp_filename, 'wt') as oh:
            oh.write(job['script'])
        os.chmod(tmp_filename, stat.S_IXUSR)
        
        # THis needs to be launched as a separate thread that become independent of this python instance.
        print(job)
        cmd = subprocess.Popen(tmp_filename,
            #stdout=job['stdout'],
            #stderr=job['stderr'],
            cwd=job['cwd'],
            shell=True,
            )
        PID = cmd.pid 
        
        self.cur.execute('UPDATE jobs SET tmp_filename=? WHERE jid=?', (tmp_filename, job['jid']))
        self.cur.execute('UPDATE jobs SET pid=? WHERE jid=?', (PID, job['jid']))
        self.con.commit()

    def process_q(self):
        """

        Here's the logic controller.

        Suggested to run every ~10s or so.

        """
        # Step 1. See if any running scripts are done.
        running_jobs = self.get_jobs_list(running_only=True)
        if running_jobs:
            for job in running_jobs:
                # check if pid is done
                print(job['pid'])

        # Step 2. Go through the list, from top to bottom, and see if any of the jobs will fit on the Q.
        waiting_jobs = self.get_jobs_list(waiting_only=True)
        node_status = self.get_node_states()

        if waiting_jobs:
            for job in waiting_jobs:
                # Can we find a space on the node?
                # TODO: Add things like node job distribution logic. 
                for node in node_status:
                    if node['status'] == 'I':
                        # Yes, free node.
                        self.allocate_job_to_node(job, node)
                        self.log.info(f'Allocated {job["jid"]} to node {node["nid"]}')
                        break
                    elif node['status'] == 'M':
                        # Okay... Need to see if space available.     
                        pass           

        return


