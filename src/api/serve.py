import configparser
import api

config = configparser.ConfigParser()
config.read("../../config/config.ini")    
app = api.backendApi(dict(config['DB']), dict(config['OTP'])).get_app()
