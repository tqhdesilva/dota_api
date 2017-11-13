import requests
import os
import json

key = os.environ['D2_API_KEY']

def get_match_history( game_mode, start_at_match_id, key=key):
    url = 'http://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v001?game_mode={game_mode}&start_at_match_id={start_at_match_id}&key={key}'\
    .format(game_mode=game_mode, start_at_match_id=start_at_match_id, key=key)
    raw = requests.get(url).content
    return json.loads(raw)

get_match_history(game_mode=2, start_at_match_id=3464261020)
