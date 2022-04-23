import configparser
import api

config = configparser.ConfigParser()
config.read("../../config/config.ini")    
app = api.backendApi(dict(config['DB'])).get_app()
