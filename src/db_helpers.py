"""Helpers for accessing psql database."""
import sqlalchemy
import pandas as pd


def connect(user, password, db, host='localhost', port=5432):
    """Create a connection and a metadata object."""
    # We connect with the help of the PostgreSQL URL
    # e.g. postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con, meta


def append_db_match_history(df, con):
    """Append match history DataFrame data to matches table."""
    df.to_sql('matches', con, dtype={'players': sqlalchemy.types.JSON},
              if_exists='append')


def append_db_match_details(df, con):
    """Append match details DataFrame data to match_details table."""
    df.to_sql('match_details', con, if_exists='append')


def build_db_match_history(con):
    """Create new matches table."""
    df = pd.DataFrame({'match_id': pd.Series(dtype='int'),
                       'start_time': pd.Series(dtype='int'),
                       'players': pd.Series()})
    df = df.set_index('match_id')
    df.to_sql('matches', con, dtype={'players': sqlalchemy.types.JSON},
              if_exists='replace')


def build_db_match_details(con):
    """Create new match_details table."""
    df = pd.DataFrame({'match_id': pd.Series(dtype='int'),
                       'radiant_win': pd.Series(dtype='bool'),
                       'duration': pd.Series(dtype='int')})
    df = df.set_index('match_id')
    df.to_sql('match_details', con, if_exists='replace')
