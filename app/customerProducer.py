from faker import Faker


def produceCustomer(orderId = 1, fake=Faker()):
    key = fake.uuid4()

    blood_group_data = fake.profile(fields=['blood_group'])
    blood_group = blood_group_data['blood_group']


    message = {
        "profile": {
            "name": fake.unique.name(),
            "job_title": fake.unique.job(),
            "blood_group": blood_group,
            "creditCardNumber": fake.credit_card_number()
        },
        "address": {
            "country": fake.country(),
            "address": fake.unique.address()
        },
        "contact": {
            "email": fake.unique.email(),
            "phone": fake.unique.phone_number()
        }
    }

    return key, message

# test and print the output
# customer_records = [produceCustomer() for _ in range(2)]
# formatted_json = json.dumps(customer_records, indent=2)
# print(formatted_json)

