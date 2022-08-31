import os
from dotenv import load_dotenv

load_dotenv('.flaskenv')

class Config(object):
    LEAFTRADE_KEY = os.environ.get('LEAFTRADE_KEY')
    EPICOR_USER = os.environ.get('EPICOR_USER')
    EPICOR_PWD = os.environ.get('EPICOR_PWD')