import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

app = Celery('symmy_integrator')
app.conf.broker_url = 'redis://redis:6379/0'
app.conf.result_backend = 'redis://redis:6379/0'
app.conf.task_routes = {
    'integrator.tasks.sync_products': {'queue': 'default'}
}
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

