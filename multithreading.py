# python data/multithreading.py <action> <db name> <duration> <start match_id> <end match_id>
# use duration = 0 for no time limit, start_matcH_id = 0 for no start_match_id
# need to test end_seq_num
# python data/multithreading.py append dota2_draft 100 3460288805 3460288707
import dota2api
from threading import Thread
from Queue import Queue, Empty, PriorityQueue
import time
import sys
import os
from api_helpers import api_call, get_match_history
from db_helpers import connect, build_db_match_history, build_db_match_details

def task(in_q, out_q, con):
    while True:
        args = in_q.get()
        api_call(args, out_q, con)
        in_q.task_done()

class Worker(object):
    def __init__(self, q_in, q_out, task):
        object.__init__(self)
        self.thread = Thread(target=self.taskrunner)
        self.q_in = q_in
        self.q_out = q_out
        self.task = task
        self.thread.daemon = True
        self.thread.start()
    def taskrunner(self):
        while True:
            args = self.q_in.get()
            result = self.task(*args)
            if result:
                self.q_out.put(result)
            self.q_in.task_done()
            self.q_out.task_done()

class Scheduler(object):
    def __init__(self, n_workers, schedule, task):
        self.schedule = schedule
        self.task_queue = Queue()
        self.heap = PriorityQueue()
        self.thread = Thread(target=self.run_tasks)
        self.workers = [Worker(self.task_queue, self.heap, task) for i in xrange(n_workers)]
        self.thread.daemon = True
        self.thread.start()
    def run_tasks(self):
        while True:
            priority, args = self.heap.get()
            self.task_queue.put(args)
            time.sleep(self.schedule)
# task spec: must return priority, valid args

if __name__ == '__main__':
    try:
        db_name = sys.argv[2]
    except IndexError:
        db_name = 'dota2_draft'
    #db_name = 'dota2_draft' # just for hydrogen
    with open(os.path.expanduser('~/.pgpass')) as f:
        for line in f:
            host, port, db, user, password = [x.strip() for x in line.split(':')]
            if db == db_name:
                con, meta = connect(user=user, password=password, db=db, host=host, port=port)
                break
    action = sys.argv[1]
    if action == 'build':
        build_db_match_details(con)
        build_db_match_history(con)
    else:
        duration = int(sys.argv[3])
        try:
            start_match_id = int(sys.argv[4])
        except IndexError:
            start_match_id = None
        if start_match_id == 0:
            start_match_id = None
        try:
            end_match_id = int(sys.argv[5])
        except IndexError:
            end_match_id = None
        append_data(start_match_id, end_match_id, duration, 4, con)
