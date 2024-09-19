
import sys
import os
import sqlite3
import time
import subprocess
import random
import stat
import threading

from .utils import convert_seconds, pid_exists, bytes_convertor2

class ThreadWithReturnValue(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, Verbose=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs) # PID

    def join(self, *args):
        threading.Thread.join(self, *args)
        return self._return

def submit_job(job, node):
    tmp_filename = f'/tmp/glurm.{time.time():.0f}.{job["jid"]}.{node["nid"]}.{job["command"]}'
    with open(tmp_filename, 'wt') as oh:
        oh.write(job['script'])
    os.chmod(tmp_filename, stat.S_IXUSR|stat.S_IRUSR)

    # TODO: Support for default join of stdout and stderr
    stderr = None
    stdout = None
    if job["stdout"]:
        stdout = open(job["stdout"], 'wt')
    if job["stderr"]:
        stderr = open(job["stderr"], 'wt')
    else:
        stderr = stdout # Both can be None

    cmd = subprocess.Popen(
        tmp_filename,
        stdout=stdout,
        stderr=stderr,
        cwd=job['cwd'],
        shell=True,
        )

    PID = cmd.pid

    return PID

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
            time_added_to_q INT,
            time_started INT,
            command TEXT,
            status TEXT,
            ncpus INT,
            memory INT,
            stdout TEXT,
            stderr TEXT,
            tmp_filename TEXT,
            node_used INT
            )
        '''

        self.cur.execute(jobs_table)

        # TODO: Sort this out to store relevant data
        finished_jobs_table = '''
        CREATE TABLE finished_jobs (
            jid INT,
            name TEXT,
            time_started INT,
            time_taken INT
            )
        '''
        self.cur.execute(finished_jobs_table)

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
            mem = bytes_convertor2('8G'), # Surpsringly no easy way to do this...
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

    def can_any_node_can_accomodate_job(self, ncpus:int=1, maxmem:int=0) -> bool:
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

    def get_job_data(self, job:dict) -> dict:
        """
        Return the node details. This is for sinfo. There is another function that packs the
        data for internal use.
        """
        self.cur.execute('SELECT * FROM jobs WHERE jid=?', (job['jid'], ))
        res = self.cur.fetchall()
        job = dict(res)
        return job

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

            if j['time_started'] != -1: #time_started:
                time_queing = convert_seconds(j['time_started'] - j['time_added_to_q'])
                time_running = convert_seconds(int(time.time() - j['time_started']))
            else:
                time_queing = convert_seconds(int(time.time() - j['time_added_to_q']))
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

    def get_node_state(self, node_name):
        """
        Return node details for a single node
        """
        self.cur.execute('SELECT * FROM node_status WHERE nid=?', (node_name,))
        node = self.cur.fetchone()

        # pack the results:
        anode = dict(node)

        anode['ncpus_avail'] = node['ncpus_total'] - node['cpus_allocated']
        anode['mem_avail'] = node['memory_total'] - node['mem_allocated']

        return anode

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
            'time_added_to_q': int(time.time()),
            'time_started': -1,
            'command': str(args.script[0]),
            'status': 'W',
            'ncpus': args.cpus_per_task,
            'memory': args.mem,
            'stdout': args.output,
            'stderr': args.error,
            'tmp_filename': '',
            'node_used': -1,
            }

        zipped = []
        for k, v in zip(sql_dict.keys(), sql_dict.values()):
            zipped.append(f':{k}')
        zipped = ', '.join(zipped)

        sql = 'INSERT INTO jobs VALUES ({})'.format(zipped)

        self.cur.execute(sql, sql_dict)
        self.con.commit()

        # JID INT PRIMARY KEY, name TEXT, time_added_to_q TEXT, time_started TEXT, command TEXT, status TEXT, ncpus INT, memory INT, stdout TEXT, stderr TEXT

    def node_can_accomodate_job(self, job, node):
        """
        Check if this job will fit on the node, return True or False

        """
        node_data = self.get_node_state(node['nid'])

        cpus_to_allocate = node_data['cpus_allocated']+job['ncpus']
        mem_to_allocate = node_data['mem_allocated']+job['memory']

        if cpus_to_allocate > node_data['ncpus_avail']:
            return False
        if mem_to_allocate > node_data['mem_avail']:
            return False

        # Can't see a reason it can't be allocated to this node;
        return True

    def allocate_job_to_node(self, job, node):
        """
        Allocate the job to a node, set all statuses in the DB, see if the node is full.

        """
        node_data = self.get_node_state(node['nid'])

        # Get our node;
        cpus_to_allocate = node_data['cpus_allocated'] + job['ncpus']
        mem_to_allocate = node_data['mem_allocated'] + job['memory']

        if cpus_to_allocate >= node_data['ncpus_total'] or mem_to_allocate > node_data['memory_total']:
            self.cur.execute('UPDATE node_status SET status="A", cpus_allocated=?, mem_allocated=? WHERE nid=?', (cpus_to_allocate, mem_to_allocate, node['nid']))
        else:
            self.cur.execute('UPDATE node_status SET status="M", cpus_allocated=?, mem_allocated=? WHERE nid=?', (cpus_to_allocate, mem_to_allocate, node['nid']))

        self.cur.execute('UPDATE jobs SET status="R", time_started=?, node_used=? WHERE jid=?', (time.time(), node['nid'], job['jid']))
        self.con.commit()

        # See if the node is now full
        node_data = self.get_node_state(node['nid'])
        if node_data['cpus_allocated'] >= node_data['ncpus_total'] or node_data["mem_allocated"] > node_data['memory_total']:
            self.cur.execute('UPDATE node_status SET status="A" WHERE nid=?', (node['nid'], ))
            self.con.commit()

        # Run the job, get a PID and update the DB
        thread = ThreadWithReturnValue(target=submit_job, args=(job, node))
        thread.start()
        PID = thread.join()
        self.log.info(f'Started {job["jid"]} with PID {PID}')

        #self.cur.execute('UPDATE jobs SET tmp_filename=?, pid=? WHERE jid=?', (tmp_filename, PID, job['jid']))
        self.cur.execute('UPDATE jobs SET pid=? WHERE jid=?', (PID, job['jid']))
        self.con.commit()

    def finish_job(self, job):
        """
        Finish the job. remove it from the job pool and free it's resources.
        """
        # copy the job details to the finished_jobs pool
        """
        CREATE TABLE finished_jobs (
            jid INT,
            name TEXT,
            time_started TEXT,
            time_taken TEXT
            )
        """
        job['time_running'] = int(job['time_started'] - time.time()) # set finish time;
        self.cur.execute('INSERT INTO finished_jobs VALUES (:jid, :name, :time_started, :time_running)', job)
        # Delete the job from the job queue
        self.cur.execute('DELETE FROM jobs WHERE jid=?', (job['jid'],))
        self.con.commit() # Make sure job is finished before proceeding;

        # Free the resources on the nodes
        cpus_freed = job['ncpus']
        mem_freed = job['memory']
        node_data = self.get_node_state(job['node_used'])

        new_cpus_allocated = node_data['cpus_allocated'] - cpus_freed
        if new_cpus_allocated < 0: new_cpus_allocated = 0 # Stop weird cases
        new_mem_allocated = node_data['mem_allocated'] - mem_freed
        if new_mem_allocated < 0: new_mem_allocated = 0 # Stop weird cases

        self.cur.execute('UPDATE node_status SET cpus_allocated=?, mem_allocated=? WHERE nid=?', (new_cpus_allocated, new_mem_allocated, job['node_used']))

        # Figure out if it's I, M or A
        if new_cpus_allocated >= node_data['ncpus_total']:
            self.cur.execute('UPDATE node_status SET status=? WHERE nid=?', ("A", job['node_used']))

        elif new_mem_allocated >= node_data['memory_total']:
            self.cur.execute('UPDATE node_status SET status=? WHERE nid=?', ("A", job['node_used']))

        elif new_mem_allocated >= 1 or new_cpus_allocated >= 1:
            self.cur.execute('UPDATE node_status SET status=? WHERE nid=?', ("M", job['node_used']))

        else:
            self.cur.execute('UPDATE node_status SET status=? WHERE nid=?', ("I", job['node_used']))

        self.con.commit()

        return

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
                if pid_exists(job['pid']):
                    self.log.info(f'Job running: JID={job["jid"]} PID={job["pid"]}')
                else:
                    self.log.info(f'Job finished: JID={job["jid"]} PID={job["pid"]}')
                    self.finish_job(job)

        # Step 2. Go through the list, from top to bottom, and see if any of the jobs will fit on the Q.
        waiting_jobs = self.get_jobs_list(waiting_only=True)

        if waiting_jobs:
            for job in waiting_jobs:
                node_status = self.get_node_states()
                # Can we find a space on the node?
                # TODO: Add things like node job distribution logic.
                for node in node_status:
                    if node['status'] == 'I' or node['status'] == 'M':
                        # Yes, free node.
                        if self.node_can_accomodate_job(job, node):
                            self.allocate_job_to_node(job, node)
                            self.log.info(f'Allocated JID={job["jid"]} to node {node["nid"]}')
                            break

        return


