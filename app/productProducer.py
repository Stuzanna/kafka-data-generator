from faker import Faker

import random

def produceProduct(orderId = 1, fake=Faker()):
    
    key = fake.uuid4()

    message = {
            "name": fake.catch_phrase(),
            "color": fake.color(),
            "price": round(random.uniform(0.01, 1000), 2)
    }

    return key, message

# # test and print the output
# customer_records = [produceProduct() for _ in range(2)]
# formatted_json = json.dumps(customer_records, indent=2)
# print(formatted_json)
