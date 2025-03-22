import threading
import time
import uuid
from kafka import KafkaConsumer
import json
import traceback
from core.broker import KafkaBroker
from core.result_backend import ResultBackend

class YADTQWorker:
    def __init__(self, kafka_host, redis_host):
        self.worker_id = str(uuid.uuid4())
        self.broker = KafkaBroker(kafka_host)
        self.backend = ResultBackend(redis_host)
        self.task_handlers = {}
        self._running = True
        
        # Register worker on startup
        self.broker.register_worker(self.worker_id)
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
        self.heartbeat_thread.start()

    def register_task(self, task_name, handler):
        self.task_handlers[task_name] = handler

    def _send_heartbeats(self):
        """Periodically send heartbeats to indicate worker is alive."""
        while self._running:
            try:
                heartbeat = {
                    'worker_id': self.worker_id,
                    'timestamp': time.time(),
                    'status': 'alive'
                }
                self.broker.producer.send('worker_heartbeats', heartbeat)
                time.sleep(5)  # Send heartbeat every 30 seconds
            except Exception as e:
                print(f"Error sending heartbeat: {e}")
                time.sleep(5)  # Wait before retrying if there's an error

    def start(self):
        consumer = KafkaConsumer(
            self.broker.task_queue_topic,
            bootstrap_servers=[self.broker.kafka_host],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='task-worker-group'
        )
        print(f"Worker {self.worker_id} started. Waiting for tasks...")
        for message in consumer:
            if not self._running:
                break
            task = message.value
            self._process_task(task)
            # Commit the offset after successful processing
            consumer.commit()

    def _process_task(self, task):
        task_id = task.get('task_id')
        task_type = task.get('task')
        args = task.get('args', [])
        print(f"Worker {self.worker_id} received task {task_id}")
        try:
            # Mark task as processing
            self.backend.set_task_status(task_id, 'processing')
            
            # Execute task
            handler = self.task_handlers.get(task_type)
            if not handler:
                raise ValueError(f"No handler found for task type: {task_type}")
            
            result = handler(*args)
            
            # Mark task as successful
            self.backend.set_task_status(task_id, 'success', result)
            print(f"Worker {self.worker_id} completed task {task_id}")
        
        except Exception as e:
            # Handle task failure
            error_message = str(e)
            error_traceback = traceback.format_exc()
            error_details = f"{error_message}\n{error_traceback}"
            
            # Set status to 'failed' with error details
            self.backend.set_task_status(task_id, 'failed', error_details)

    def stop(self):
        """Gracefully stop the worker"""
        self._running = False
        print(f"Worker {self.worker_id} stopping...")