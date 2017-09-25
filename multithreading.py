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
from data.api_helpers import api_call, get_match_history
from data.db_helpers import connect, build_db_match_history, build_db_match_details

def task(in_q, out_q, con):
    while True:
        args = in_q.get()
        api_call(args, out_q, con)
        in_q.task_done()

class Worker(object):
    def __init__(self, task, args=None):
        object.__init__(self)
        self.thread = Thread(target=task, args=args)
        self.thread.daemon = True
        self.thread.start()

class Scheduler(object):
    def __init__(self, task_queue, schedule):
        self.schedule = schedule
        self.task_queue = task_queue
        self.heap = PriorityQueue()
        self.thread = Thread(target=self.run_tasks)
        self.thread.daemon = True
        self.thread.start()
    def run_tasks(self):
        while True:
            priority, item = self.heap.get()
            self.task_queue.put(item)
            time.sleep(self.schedule)

def append_data(start_match_id, end_match_id, duration, num_workers, con):
        workers = []
        q = Queue()
        scheduler = Scheduler(q, 1)
        time0 = time.time()
        for i in xrange(num_workers):
            workers.append(Worker(task=task, args=(q, scheduler.heap, con)))
        scheduler.heap.put((3, (get_match_history, start_match_id,
                                end_match_id, time0, duration)))
        scheduler.heap.join()
        q.join()
        print('done')
        print('ran for {} seconds'.format(time.time() - time0))

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
