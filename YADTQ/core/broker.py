from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import uuid
import signal
import sys

class KafkaBroker:
    def __init__(self, kafka_host):
        self.kafka_host = kafka_host
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_host],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.task_queue_topic = 'task_queue'
        self.worker_queue_topic = 'worker_queue'
        self.heartbeat_topic = 'worker_heartbeats'  # New topic for heartbeats
        self.running = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print("\nShutting down Kafka broker...")
        self.running = False
        sys.exit(0)

    def start(self):
        print("Starting Kafka broker...")
        print("Press Ctrl+C to stop")
        task_consumer = KafkaConsumer(
            self.task_queue_topic,
            bootstrap_servers=[self.kafka_host],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='broker-task-group'
        )
        worker_consumer = KafkaConsumer(
            self.worker_queue_topic,
            bootstrap_servers=[self.kafka_host],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='broker-worker-group'
        )
        heartbeat_consumer = KafkaConsumer(
            self.heartbeat_topic,
            bootstrap_servers=[self.kafka_host],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='broker-heartbeat-group'
        )

        task_thread = threading.Thread(target=self._monitor_tasks, args=(task_consumer,))
        worker_thread = threading.Thread(target=self._monitor_workers, args=(worker_consumer,))
        heartbeat_thread = threading.Thread(target=self._monitor_heartbeats, args=(heartbeat_consumer,))
        
        task_thread.daemon = True
        worker_thread.daemon = True
        heartbeat_thread.daemon = True
        
        task_thread.start()
        worker_thread.start()
        heartbeat_thread.start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down Kafka broker...")
            self.running = False

    def _monitor_tasks(self, consumer):
        while self.running:
            try:
                for message in consumer:
                    if not self.running:
                        break
                    task = message.value
                    print(f"New task received: {task}")
            except Exception as e:
                print(f"Error in task monitoring: {e}")
            if self.running:
                time.sleep(1)

    def _monitor_workers(self, consumer):
        print("Started monitoring workers...")
        while self.running:
            try:
                for message in consumer:
                    if not self.running:
                        break
                    try:
                        worker_data = message.value
                        worker_id = worker_data.get('worker_id')
                        if worker_id:
                            print(f"Worker registered: {worker_id}")
                        else:
                            print(f"Malformed worker registration message: {worker_data}")
                    except Exception as e:
                        print(f"Error processing worker message: {e}")
            except Exception as outer_exception:
                print(f"Error in worker monitoring loop: {outer_exception}")
            if self.running:
                time.sleep(1)

    def _monitor_heartbeats(self, consumer):
        """Monitor and print worker heartbeats."""
        print("Started monitoring worker heartbeats...")
        while self.running:
            try:
                for message in consumer:
                    if not self.running:
                        break
                    try:
                        heartbeat_data = message.value
                        worker_id = heartbeat_data.get('worker_id')
                        timestamp = heartbeat_data.get('timestamp')
                        if worker_id and timestamp:
                            print(f"Heartbeat received from worker {worker_id} at {timestamp}")
                        else:
                            print(f"Malformed heartbeat message: {heartbeat_data}")
                    except Exception as e:
                        print(f"Error processing heartbeat message: {e}")
            except Exception as outer_exception:
                print(f"Error in heartbeat monitoring loop: {outer_exception}")
            if self.running:
                time.sleep(1)

    def send_task(self, task_type, args):
        task_id = str(uuid.uuid4())
        task = {
            'task_id': task_id,
            'task': task_type,
            'args': args
        }
        self.producer.send(self.task_queue_topic, task)
        print(f"Task sent to queue: {task_id}")
        return task_id

    def register_worker(self, worker_id):
        """Send a worker registration message."""
        worker_data = {
            'worker_id': worker_id,
            'timestamp': time.time()
        }
        self.producer.send(self.worker_queue_topic, worker_data)
        print(f"Worker {worker_id} registered")