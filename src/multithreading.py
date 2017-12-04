"""Multithreading class and function definitions."""
from threading import Thread
from Queue import Queue, Empty, PriorityQueue
import time
from api_helpers import api_call


def task(in_q, out_q, con):
    """Call API on args and handle queues."""
    while True:
        args = in_q.get()
        api_call(args, out_q, con)
        in_q.task_done()


class Worker(object):
    """Worker class for concurrent tasks."""

    def __init__(self, q_in, q_out, task):
        """Initialize object."""
        object.__init__(self)
        self.thread = Thread(target=self.taskrunner)
        self.q_in = q_in
        self.q_out = q_out
        self.task = task
        self.thread.daemon = True
        self.alive = True
        self.thread.start()

    def taskrunner(self):
        """Run task while alive."""
        while self.alive:
            try:
                args = self.q_in.get(timeout=.1)
            except Empty:
                continue
            result = self.task(*args)
            if result:
                for r in result:
                    self.q_out.put(r)
            self.q_in.task_done()
            self.q_out.task_done()
        return

    def kill(self):
        """Stop running task."""
        self.alive = False


class Scheduler(object):
    """Scheduler object for managing concurrent worker schedules."""

    def __init__(self, n_workers, schedule, task):
        """Initialize scheduler."""
        self.schedule = schedule
        self.task_queue = Queue()
        self.heap = PriorityQueue()
        self.thread = Thread(target=self.run_tasks)
        self.workers = [Worker(self.task_queue, self.heap, task) for i in xrange(n_workers)]
        self.thread.daemon = True
        self.alive = True
        self.thread.start()

    def run_tasks(self):
        """Move tasks from heap to active task queue on timer."""
        while self.alive:
            try:
                priority, args = self.heap.get(timeout=.1)
            except Empty:
                continue
            self.task_queue.put(args)
            time.sleep(self.schedule)
        return

    def kill(self):
        """End Scheduler."""
        self.alive = False
# task spec: must return priority, valid args
