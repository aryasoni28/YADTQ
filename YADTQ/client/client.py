from core.broker import KafkaBroker
from core.result_backend import ResultBackend

class YADTQClient:
    def __init__(self, kafka_host, redis_host):
        self.broker = KafkaBroker(kafka_host)
        self.backend = ResultBackend(redis_host)

    def send_task(self, task_type, args):
        task_id = self.broker.send_task(task_type, args)
        self.backend.set_task_status(task_id, 'queued')
        return task_id

    def get_status(self, task_id):
        task_data = self.backend.get_task_status(task_id)
        if task_data:
            return task_data['status']
        return None

    def get_result(self, task_id):
        task_data = self.backend.get_task_status(task_id)
        if task_data and (task_data['status'] == 'success' or task_data['status'] == 'failed'):
            return task_data['result']
        return None