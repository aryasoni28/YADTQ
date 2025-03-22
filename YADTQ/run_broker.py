# run_broker.py
from core.broker import KafkaBroker

def main():
    broker = KafkaBroker(kafka_host='localhost:9092')
    try:
        broker.start()
    except KeyboardInterrupt:
        print("\nBroker shutdown complete")

if __name__ == '__main__':
    main()