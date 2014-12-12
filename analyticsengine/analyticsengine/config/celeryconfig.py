__author__ = 'sarink'

# config file for Celery Daemon

# default Redis broker
BROKER_URL = 'redis://localhost'

# default RabbitMQ backend
CELERY_RESULT_BACKEND = 'redis://localhost'