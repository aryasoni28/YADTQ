# core/result_backend.py
import redis
import json
import signal
import sys
import time
import threading

class ResultBackend:
    def __init__(self, redis_host):
        self.redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        self.running = True
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print("\nShutting down Result Backend...")
        self._clear_redis_data()  # Clear all tasks from Redis
        self.running = False
        sys.exit(0)

    def _clear_redis_data(self):
        """Clear all task data from Redis"""
        try:
            all_tasks = self.redis_client.keys('*')
            for task_id in all_tasks:
                self.redis_client.delete(task_id)
            print("Cleared all task data from Redis.")
        except Exception as e:
            print(f"Error clearing Redis data: {e}")

    def start(self):
        print("Starting Result Backend...")
        print("Monitoring task statuses...")
        print("Press Ctrl+C to stop")

        # Start monitoring thread
        monitor_thread = threading.Thread(target=self._monitor_tasks)
        monitor_thread.daemon = True
        monitor_thread.start()

        # Keep main thread running
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down Result Backend...")
            self.running = False

    def _monitor_tasks(self):
        """Monitor task status changes"""
        while self.running:
            try:
                # Get all task keys
                all_tasks = self.redis_client.keys('*')
                
                for task_id in all_tasks:
                    task_data = self.get_task_status(task_id)
                    if task_data:
                        status = task_data.get('status')
                        result = task_data.get('result')
                        print(f"Task {task_id}: Status={status}, Result={result}")
                
                time.sleep(1)  # Check every second
                
            except Exception as e:
                print(f"Error in task status monitoring: {e}")
                if self.running:
                    time.sleep(1)  # Wait before retrying

    def set_task_status(self, task_id, status, result=None):
        task_data = {
            'status': status,
            'result': result
        }
        self.redis_client.set(task_id, json.dumps(task_data))
        print(f"Updated task {task_id}: Status={status}, Result={result}")

    def get_task_status(self, task_id):
        task_data = self.redis_client.get(task_id)
        if task_data:
            return json.loads(task_data)
        return None