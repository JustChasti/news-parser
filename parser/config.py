from os import getenv

""" Настройки базы """

base_user = getenv('POSTGRES_USER')
base_pass = getenv('POSTGRES_PASSWORD')
base_name = getenv('POSTGRES_DB')
base_host = getenv('POSTGRES_HOST')
base_port = getenv('POSTGRES_PORT')

""" Задержка в минутах, максимум поиска по категориям,
максимальная глубина для однокатегориийного сайта
 """

delay = 60 * 10
max_deep_cat = 20
max_deep = 50
