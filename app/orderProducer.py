import random

from faker import Faker
MAX_PIZZAS_IN_ORDER = 10
MAX_ADDITIONAL_TOPPINGS_IN_PIZZA = 5



def producePizzaOrder(orderId = 1, fake=Faker()):
    shop = fake.pizzaShop()
    pizzas = []
    for pizza in range(random.randint(1,MAX_PIZZAS_IN_ORDER)):
        toppings = []
        for topping in range(random.randint(1,MAX_ADDITIONAL_TOPPINGS_IN_PIZZA)):
            toppings.append(fake.pizzaTopping())
        pizzas.append({
            "pizzaName": fake.pizzaName(),
            "additionalToppings": toppings
        })
    
    message = {
        "id": orderId,
        "shop": shop,
        "customerName": fake.unique.name(),
        "phoneNumber": fake.unique.phone_number(),
        "address": fake.unique.address(),
        "pizzas": pizzas
    }

    key = shop

    return key,message