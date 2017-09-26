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

def build_match_sequence(con):
    df = pd.DataFrame({'match_id': pd.Series(dtype='int'),
                       'start_time' : pd.Series(dtype='int'),
                       'match_seq_num' : pd.Series(dtype='int'),
                       'radiant_win' : pd.Series(dtype='bool'),
                       'game_mode' : pd.Series(dtype='int'),
                       'duration': pd.Series(dtype='int'),
                       'players': pd.Series()})
    df = df.set_index('match_id')
    df.to_sql('matches', con, dtype={ 'players' : sqlalchemy.types.JSON}, if_exists='replace')

def get_match_sequence(game_mode, start_at_match_sequence, matches_requested, end_seq_num, con):
    try:
        response = api.get_match_history_by_seq_num(start_at_match_seq_num=start_at_match_sequence,
                                                    matches_requested=matches_requested)['matches']
    except:
        # print('Valve shits the bed...')
        time.sleep(4)
        args = (game_mode, start_at_match_sequence, matches_requested, end_seq_num, con)
        return (0, args)

    fields = ['match_id', 'match_seq_num', 'start_time', 'radiant_win', 'players', 'game_mode', 'duration']
    data = map(lambda x: [x[field] for field in fields], response)
    df = pd.DataFrame(data=data, columns=fields)
    if end_seq_num:
        df = df[df['match_seq_num'] <= end_seq_num]
    if df.empty:
        return

    max_match_seq_num = df['match_seq_num'].max()

    cm = df[df['game_mode'] == game_mode][fields].set_index('match_id')
    if not cm.empty:
         cm.to_sql('matches', con,
                dtype={'players' : sqlalchemy.types.JSON},
                if_exists='append')

    args = (game_mode, max_match_seq_num + 1, matches_requested, end_seq_num, con)
    return (1, args)



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
        build_match_sequence(con)
    else:
        try:
            start_match_id = int(sys.argv[3])
        except IndexError:
            start_match_id = None
        if start_match_id == 0:
            start_match_id = None
        try:
            end_match_id = int(sys.argv[4])
        except IndexError:
            end_match_id = None
        append_data(2, start_match_id, end_match_id, 4, con)
