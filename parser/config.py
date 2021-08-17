from os import getenv


base_user = getenv('POSTGRES_USER')
base_pass = getenv('POSTGRES_PASSWORD')
base_name = getenv('POSTGRES_DB')
base_host = getenv('POSTGRES_HOST')
base_port = getenv('POSTGRES_PORT')


delay = 60 * int(getenv('DELAY'))
max_deep_cat = 20
max_deep = 50
