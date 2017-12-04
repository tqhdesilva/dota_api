"""Helpers for using dota2api library."""
import datetime
import pandas as pd
from collections import defaultdict
import time
import os
from db_helpers import append_db_match_history, append_db_match_details
import dota2api


api = dota2api.Initialise(os.environ['D2_API_KEY'])


def parse_date(date):
    """Parse date string to tuple."""
    return int(time.mktime(datetime.datetime.strptime(date, '%Y-%m-%d').timetuple()))


def api_call(args, out_q, con):
    """Parse function and args, pass queue and connection."""
    method = args[0]
    args = args[1:]
    method(*args, out_q=out_q, con=con)
    out_q.task_done()


def parse_match_history(result, end_match_id):
    """Parse dota2api response to DataFrame."""
    parsed_result = result['matches']
    df_dict = defaultdict(list)
    keys = ['match_id', 'players', 'start_time']
    for row in parsed_result:
        for key in keys:
            df_dict[key].append(row[key])
    df = pd.DataFrame(df_dict, columns=keys)
    if end_match_id != None:
        df = df[df['match_id'] >= end_match_id]
    # because dota 2 web api sucks donkey dick, we need to manually filter for
    # matches where player count == 10 (not necessarily AP games)
    # hopefully for any given set of 500 matches at least one has 10 players,
    # otherwise our program breaks
    # the API decides to randomly work sometimes and game_mode=2 returns different
    # results for the same starting_match_id
    # wtf valve
    df = df[df['players']]
    df = df.set_index('match_id')
    return df


def parse_match_details(result):
    keys = ['match_id', 'radiant_win', 'duration']
    parsed_result = {key : [result[key]] for key in keys}
    df = pd.DataFrame(parsed_result, columns=keys)
    df = df.set_index('match_id')
    return df


def get_match_history(start_match_id, end_match_id, time0, duration, out_q, con):
    """Query match history and add match IDs to job queue."""
    try:
        result = api.get_match_history(game_mode=2, start_at_match_id=start_match_id)
    except ValueError:
        args = (get_match_history, start_match_id, end_match_id, time0, duration)
        out_q.put((1, args))
        return
    result_df = parse_match_history(result, end_match_id)
    append_db_match_history(result_df, con)
    for match_id in result_df.index:
        out_q.put((2, (get_match_details, match_id)))
    if (duration <= 0 or time.time() - time0 < duration - result_df.shape[0])\
            and result_df.shape[0] > 0:
        args = (get_match_history, result_df.index.min() - 1,
                end_match_id, time0, duration)
        out_q.put((3, args))


def get_match_details(match_id, out_q, con):
    """Query match details and write response to database."""
    try:
        result = api.get_match_details(match_id)
    except ValueError:
        args = (get_match_details, match_id)
        out_q.put((1, args))
        return

    result_df = parse_match_details(result)
    append_db_match_details(result_df, con)
