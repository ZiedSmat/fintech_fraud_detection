import json
import random
import time
from faker import Faker
from datetime import datetime

def generate_fraud_transactions(num_transactions=100):
    """Generates simulated fraud transactions using Faker."""
    fake = Faker()
    transactions = []

    for _ in range(num_transactions):
        transaction = {
            "id": fake.uuid4(),
            "amount": random.randint(10001, 20000),
            "location": fake.city(),
            "timestamp": time.time()
        }
        transactions.append(transaction)
    
    return transactions

if __name__ == "__main__":
    data = generate_fraud_transactions(100)
    
    readable_data = []
    for d in data[:3]:
        readable_tx = d.copy() 
        readable_tx['timestamp'] = datetime.fromtimestamp(d['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        readable_data.append(readable_tx)
    
    print("Sample Output:")
    print(json.dumps(readable_data, indent=4))
    print(f"\nTotal transactions generated: {len(data)}")