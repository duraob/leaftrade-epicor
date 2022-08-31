from flask import Flask
from config import Config

UPLOAD_FOLDER = '/'

app = Flask(__name__)
app.config.from_object(Config)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

from app import routes, errors