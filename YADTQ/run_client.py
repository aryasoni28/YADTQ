from client.client import YADTQClient
import time

def main():
    client = YADTQClient(kafka_host='localhost:9092', redis_host='localhost')
    
    while True:
        print("\nYADTQ Task Queue Client")
        print("1. Submit new task")
        print("2. Check task status")
        print("3. Exit")
        
        choice = input("Enter your choice (1-3): ")
        
        if choice == '1':
            print("\nAvailable operations:")
            print("1. Add")
            print("2. Multiply")
            print("3. Divide")
            
            op_choice = input("Choose operation (1-3): ")
            num1 = float(input("Enter first number: "))
            num2 = float(input("Enter second number: "))
            
            task_type = {
                '1': 'add',
                '2': 'multiply',
                '3': 'divide'
            }.get(op_choice)
            
            if task_type:
                task_id = client.send_task(task_type, [num1, num2])
                print(f"\nTask submitted with ID: {task_id}")
                print("Initial status: queued")
            
        elif choice == '2':
            task_id = input("Enter task ID: ")
            status = client.get_status(task_id)
            if status == 'success' or status == 'failed':
                result = client.get_result(task_id)
                print(f"Status: {status}")
                print(f"Result: {result}")
            elif status:
                print(f"Status: {status}")
            else:
                print("Task not found")
                
        elif choice == '3':
            break

if __name__ == '__main__':
    main()