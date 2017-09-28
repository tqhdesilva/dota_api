from multithreading import Scheduler
import dota2api
import os
import sys
import pandas as pd
from db_helpers import connect
import sqlalchemy
import time
from collections import defaultdict

api = dota2api.Initialise(os.environ['D2_API_KEY'])

def build_match_history(con):
    df = pd.DataFrame({'match_id': pd.Series(dtype='int'),
                        'match_seq_num': pd.Series(dtype='int'),
                       'start_time' : pd.Series(dtype='int'),
                       'radiant_win' : pd.Series(dtype='bool'),
                       'game_mode' : pd.Series(dtype='int'),
                       'duration': pd.Series(dtype='int'),
                       'players': pd.Series()
                       })
    df = df.set_index('match_id')
    df.to_sql('match_history', con, dtype={ 'players' : sqlalchemy.types.JSON},
              if_exists='replace')

def get_match_history(start_at_match_id, end_at_match_id, matches_requested, con):
    try:
        result = api.get_match_history(start_at_match_id=start_at_match_id,
                                       skill=3, min_players=10,
                                       matches_requested=matches_requested)['matches']
    except:
        time.sleep(2)
        args = (get_match_history, start_at_match_id, end_at_match_id, matches_requested, con)
        return [(0, args)]
    match_ids = map(lambda x: x['match_id'], result)
    if end_at_match_id:
        match_ids = filter(lambda x: x >= end_at_match_id, match_ids)

    match_ids = filter(lambda x: pd.read_sql('SELECT match_id FROM match_history WHERE match_id = {}'.format(x), con).empty, match_ids) # filter ones we already have
    if len(match_ids) == 0:
        return

    argses = [(get_match_details, match_id, con) for match_id in match_ids]
    next_match_id = min(match_ids) - 1
    hist_args = (get_match_history, next_match_id, end_at_match_id, matches_requested, con)
    return [(1, args) for args in argses] + [(2, hist_args)]

def get_match_details(match_id, con):
    try:
        result = api.get_match_details(match_id)
    except:
        time.sleep(2)
        args = (get_match_details, match_id, con)
        return [(0, args)]
    for x in result:
        result[x] = [result[x]]
    df = pd.DataFrame(data=result)
    df = df[['match_id', 'match_seq_num', 'start_time', 'radiant_win', 'game_mode', 'duration', 'players']]
    df = df.set_index('match_id')
    df.to_sql('match_history', con, dtype={ 'players' : sqlalchemy.types.JSON },
              if_exists='append')

def getter(func, *args):
    return func(*args)


def append_history(start_at_match_id, end_at_match_id, n_workers, con):
    scheduler = Scheduler(n_workers=n_workers, schedule=1, task=getter)
    time0 = time.time()
    args = (get_match_history, start_at_match_id, end_at_match_id, 1000, con)
    scheduler.heap.put((2, args))
    scheduler.heap.join()
    scheduler.task_queue.join()
    print('done')
    print('ran for {} seconds'.format(time.time() - time0))

def append_data(game_mode, start_match_seq_num, end_match_seq_num, num_workers, con):
    scheduler = Scheduler(n_workers=num_workers, schedule=1, task=get_match_sequence)
    time0 = time.time()
    args = (game_mode, start_match_seq_num, 1000, end_match_seq_num, con)
    scheduler.heap.put((1, args))
    scheduler.heap.join()
    scheduler.task_queue.join()
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
        build_match_history(con)
    else:
        try:
            start_at_match_id = int(sys.argv[3])
        except IndexError:
            start_at_match_id = None
        if start_at_match_id == 0:
            start_at_match_id = None
        try:
            end_at_match_id = int(sys.argv[4])
        except IndexError:
            end_at_match_id = None
        append_history(start_at_match_id, end_at_match_id, 4, con)
