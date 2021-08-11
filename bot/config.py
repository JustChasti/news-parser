from os import getenv

""" Настройки базы """

base_user = getenv('POSTGRES_USER')
base_pass = getenv('POSTGRES_PASSWORD')
base_name = getenv('POSTGRES_DB')
base_host = getenv('POSTGRES_HOST')
base_port = getenv('POSTGRES_PORT')

""" Телега """

token = getenv('TOKEN')
