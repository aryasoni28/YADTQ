# run_result_backend.py
from core.result_backend import ResultBackend

def main():
    backend = ResultBackend(redis_host='localhost')
    try:
        backend.start()
    except KeyboardInterrupt:
        print("\nResult Backend shutdown complete")

if __name__ == '__main__':
    main()