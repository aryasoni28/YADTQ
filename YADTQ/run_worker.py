from worker.worker import YADTQWorker
import time

def add(a, b):
    time.sleep(15)  # Simulate work
    return a + b

def multiply(a, b):
    time.sleep(15)  # Simulate work
    return a * b

def divide(a, b):
    time.sleep(15)  # Simulate work
    if b == 0:
        raise ValueError("Division by zero!")
    return a / b

def main():
    worker = YADTQWorker(kafka_host='localhost:9092', redis_host='localhost')
    
    # Register task handlers
    worker.register_task('add', add)
    worker.register_task('multiply', multiply)
    worker.register_task('divide', divide)
    
    print(f"Worker started with ID: {worker.worker_id}")
    print("Registered tasks: add, multiply, divide")
    print("Waiting for tasks...")
    
    worker.start()

if __name__ == '__main__':
    main()