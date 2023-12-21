
import random

from faker.providers import BaseProvider

class PizzaProvider(BaseProvider):
    def pizzaName(self):
        validPizzaNames = [
            "Margheritta",
            "Marinara",
            "Diavola",
            "Mari & Monti",
            "Salami",
            "Peppperoni"
        ]
        return validPizzaNames[random.randint(0, len(validPizzaNames)-1)]
    
    def pizzaTopping(self):
        availablePizzaToppings = [
            "🍅 tomato",
            "🧀 blue cheese",
            "🥚 egg",
            "🫑 green peppers",
            "🌶️ hot pepper",
            "🥓 bacon",
            "🫒 olives",
            "🧄 garlic",
            "🐟 tuna",
            "🧅 onion",
            "🍍 pineapple",
            "🍓 strawberry",
            "🍌 banana",
        ]
        return random.choice(availablePizzaToppings)
    
    def pizzaShop(self):
        pizzaShops = [
            "Marios Pizza",
            "Luigis Pizza",
            "Circular Pi Pizzeria",
            "Ill Make You a Pizza You Can" "t Refuse",
            "Mammamia Pizza",
            "Its-a me! Mario Pizza!",
        ]
        return random.choice(pizzaShops)
        