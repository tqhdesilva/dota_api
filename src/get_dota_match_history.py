"""Script to concurrently query Valve's API and save data to psql.

Usage:
python get_dota_match_history <action> <dbname> <start_at_match_id>
    <end_at_match_id>

This script can query either get_match_history endpoint or
get_match_history_by_seq_num endpoint. The actual requests are handled by
dota2api library.

"""
from multithreading import Scheduler
import dota2api
import os
import sys
import pandas as pd
from db_helpers import connect
import sqlalchemy
import time

api = dota2api.Initialise(os.environ['D2_API_KEY'])


def build_match_history(con):
    """Build match history database.

    Has match_id, match_seq_num, start_time, radiant_win, game_mode, duration,
    and players fields. This function is useful for overwriting a database
    without having to manually do so in psql.

    Arguments:
        con: SQLAlchemy connection object of psql database.
    """
    df = pd.DataFrame({'match_id': pd.Series(dtype='int'),
                       'match_seq_num': pd.Series(dtype='int'),
                       'start_time': pd.Series(dtype='int'),
                       'radiant_win': pd.Series(dtype='bool'),
                       'game_mode': pd.Series(dtype='int'),
                       'duration': pd.Series(dtype='int'),
                       'players': pd.Series()
                       })
    df = df.set_index('match_id')
    df.to_sql('match_history', con, dtype={'players': sqlalchemy.types.JSON},
              if_exists='replace')


def get_match_history(start_at_match_id, end_at_match_id, matches_requested,
                      con):
    """Get match history function.

    Gets matches in the high skill bracket with 10 players. Note that the API
    endpoint only works for recent matches. If older matches are queried then
    the function will return nothing. This function checks to also filters out
    matches which are already in the database.

    Arguments:
        start_at_match_id (int): Match ID to start gathering matches.
        end_at_match_id (int): Match ID to stop gathering matches. This should
        be less than start_at_match_id since matches are grabbed newest to
        oldest.
        mathes_requested (int): Limit to number of matches to request.
        con: SQLAlchemy connection object to database to check duplicates.

    Returns:
        result (list): List of tuples. Each tuple looks like (priority, args)
        where priority is the priority of the task in the priority queue.
        args is (function, *args) where function is the function the worker
        will execute and *args is the expanded list of args that function
        takes. Returns None if no match IDs meet specifications.

    """
    try:
        result = api.get_match_history(start_at_match_id=start_at_match_id,
                                       skill=3, min_players=10,
                                       matches_requested=matches_requested)['matches'] # noqa
    except: # noqa
        time.sleep(2)
        args = (get_match_history, start_at_match_id,
                end_at_match_id, matches_requested, con)
        return [(0, args)]
    match_ids = map(lambda x: x['match_id'], result)
    if end_at_match_id:
        match_ids = filter(lambda x: x >= end_at_match_id, match_ids)

    match_ids = filter(lambda x: pd.read_sql(
                                             '''SELECT match_id FROM match_history
                                                WHERE match_id = {}'''
                                             .format(x), con).empty, match_ids)
    if len(match_ids) == 0:
        return

    argses = [(get_match_details, match_id, con) for match_id in match_ids]
    next_match_id = min(match_ids) - 1
    hist_args = (get_match_history, next_match_id,
                 end_at_match_id, matches_requested, con)
    return [(1, args) for args in argses] + [(2, hist_args)]


def get_match_details(match_id, con):
    """Get match details endpoint.

    Gets the match details for a single match and appends it to database.

    Parameters:
        match_id (int): The match ID you want details for.
        con: SQLAlchemy connection of the database to append to.

    """
    try:
        result = api.get_match_details(match_id)
    except: # noqa
        time.sleep(2)
        args = (get_match_details, match_id, con)
        return [(0, args)]
    for x in result:
        result[x] = [result[x]]
    df = pd.DataFrame(data=result)
    df = df[['match_id', 'match_seq_num', 'start_time', 'radiant_win',
             'game_mode', 'duration', 'players']]
    df = df.set_index('match_id')
    df.to_sql('match_history', con, dtype={ 'players' : sqlalchemy.types.JSON }, # noqa
              if_exists='append')


def getter(func, *args):
    """Just small function to take our task tuple and execute it.

    Parameters:
        func: Function to execute
        *args: arguments for function

    """
    return func(*args)


def append_history(start_at_match_id, end_at_match_id, n_workers, con):
    """Append data to database by get match history endpoint.

    """
    scheduler = Scheduler(n_workers=n_workers, schedule=1, task=getter)
    time0 = time.time()
    args = (get_match_history, start_at_match_id, end_at_match_id, 1000, con)
    scheduler.heap.put((2, args))
    scheduler.heap.join()
    scheduler.task_queue.join()
    for worker in scheduler.workers:
        worker.kill()
        worker.thread.join()
    scheduler.kill()
    scheduler.thread.join()
    print('done')
    print('ran for {} seconds'.format(time.time() - time0))


def get_match_sequence(game_mode, start_at_match_sequence, matches_requested,
                       end_seq_num, con):
    """Get matches by sequence number.

    Parameters:

    """
    try:
        response = api.get_match_history_by_seq_num(
                    start_at_match_seq_num=start_at_match_sequence,
                    matches_requested=matches_requested
        )['matches']
    except:
        time.sleep(2)
        args = (get_match_sequence, game_mode, start_at_match_sequence,
                matches_requested, end_seq_num, con)
        return (0, args)

    fields = ['match_id', 'match_seq_num', 'start_time', 'radiant_win',
              'players', 'game_mode', 'duration', 'picks_bans']
    data = map(lambda x: [x[field] if field in x else None for field in fields], # noqa
               response)
    df = pd.DataFrame(data=data, columns=fields)
    if end_seq_num:
        df = df[df['match_seq_num'] <= end_seq_num]
    if df.empty:
        return


def append_seq(game_mode, start_match_seq_num, end_match_seq_num, num_workers,
               con):
    """Append match data by sequence number to a database concurrently.

    Parameters:
        game_mode (int): Game mode ID to filter by
        start_match_seq_num (int): Sequence number to start, ascending.
        end_match_seq_num (int): Sequence number to end at, ascending.
        num_workers (int): Number of workers to spawn for this task.
        con: SQLAlchemy connection object for storing data.

    """
    scheduler = Scheduler(n_workers=num_workers, schedule=1,
                          task=get_match_sequence)
    time0 = time.time()
    args = (game_mode, start_match_seq_num, 100, end_match_seq_num, con)
    scheduler.heap.put((1, args))
    scheduler.heap.join()
    scheduler.task_queue.join()
    for worker in scheduler.workers:
        worker.kill()
        worker.thread.join()
    scheduler.kill()
    scheduler.thread.join()
    print('done')
    print('ran for {} seconds'.format(time.time() - time0))


if __name__ == '__main__':
    try:
        db_name = sys.argv[2]
    except IndexError:
        db_name = 'dota2_draft'
    # db_name = 'dota2_draft' # just for hydrogen
    with open(os.path.expanduser('~/.pgpass')) as f:
        for line in f:
            host, port, db, user, password \
                = [x.strip() for x in line.split(':')]
            if db == db_name:
                con, meta = connect(user=user, password=password, db=db,
                                    host=host, port=port)
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
