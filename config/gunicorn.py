# gunicorn config
# gunicorn -c config/gunicorn.py --worker-class sanic.worker.GunicornWorker server:app
from config import Config

bind = '0.0.0.0:7503'
backlog = 2048

workers = Config.WORKER_NUM
worker_connections = 1000
timeout = 60
keepalive = 2

spew = False
daemon = False
umask = 0
# preload = True
