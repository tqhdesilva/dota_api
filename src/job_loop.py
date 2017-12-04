"""Execute get_dota_match_history and sleep two minutes on failure."""
import os
import sys
import time
from get_dota_match_history import append_history
from db_helpers import connect

if __name__ == '__main__':
    try:
        db_name = sys.argv[1]
    except IndexError:
        db_name = 'dota2_draft'
    with open(os.path.expanduser('~/.pgpass')) as f:
        for line in f:
            host, port, db, user, password = [x.strip() for x in line.split(':')]
            if db == db_name:
                con, meta = connect(user=user, password=password, db=db, host=host, port=port)
                break
    try:
        start_at_match_id = int(sys.argv[2])
    except IndexError:
        start_at_match_id = None
    if start_at_match_id == 0:
        start_at_match_id = None
    try:
        end_at_match_id = int(sys.argv[3])
    except IndexError:
        end_at_match_id = None
    while True:
        print('starting')
        append_history(start_at_match_id, end_at_match_id, 4, con)
        time.sleep(120)
