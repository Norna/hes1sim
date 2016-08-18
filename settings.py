import os

HEALTH_INTEVAL_TIME = 60

HOST= os.environ.get("WEHA_APP_HOST")
#os.environ["WEHA_APP_HOST"]

PORT= os.environ.get("WEHA_APP_PORT")
#os.environ["WEHA_APP_PORT"]

MANAGER_ADDRESS = os.environ.get("WEHA_API_HOST")

MANAGER_PORT = os.environ.get("WEHA_API_PORT")

DJANGO_SERVER_PATH = ''

SECRET_KEY = 'n(bd1f1c%e8=_xad02x5qtfn%wgwpi492e$8_erx+d)!tpeoim'

APP_NAME = 'sample'

HEALTH_PATH = 'health/report/'

IGNORE_API = False

MF_USERNAME = os.environ.get("WEHA_API_USERNAME")

MF_PASSWORD = os.environ.get("WEHA_API_PASSWORD")

INSTALLED_SERVICES = (
    'hes1wrapper.Hes1Wrapper',
)

NO_THREADING = False
