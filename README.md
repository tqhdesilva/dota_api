# Setup

Make sure you set up your postgres database and add it to your pgpass. Also set up your environmental variables to have your
dota 2 API key.

You will need to refactor the code or use an older commit to use the sequential match endpoint.

Run get_dota_match_history.py to get match history.

python get_dota_match_history.py append <db_name> <start> <stop>

Keep in mind that the get_match_history endpoint won't always work with older matches(it'll return an empty list).

Run job_loop to continuously run queries on get_match_history.

python job_loop.py <db_name>

is the common usage to just grab the most recent matches.
