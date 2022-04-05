import configparser
import api

#to serve, use:
# uvicorn serve:app --reload --port 8001 

config = configparser.ConfigParser()
config.read("../../config.ini")    
app = api.backendApi(dict(config['DB'])).get_app()
