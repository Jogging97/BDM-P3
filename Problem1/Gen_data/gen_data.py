import numpy as np
import pandas as pd
import random
from faker import Faker

fake = Faker()
Faker.seed(123)
random.seed(123)

def dataset_p1_c():
    id = []
    name = []
    age = []
    code = []
    slry = []

    n = 50
    for i in range(n):
        id.append(i+1)
        name.append(fake.name())

        agenum = random.randint(18, 100)
        age.append(agenum)

        codenum = random.randint(1, 100)
        code.append(codenum)

        slrynum = random.randint(100, 10000000)
        slry.append(slrynum)

    print(i+1,'records')

    d1 = pd.DataFrame({
        'ID': id,
        'Name': name,
        'Age': age,
        'CountryCode': code,
        'Salary': slry
    })
    d1.to_csv('./Customers.csv', header=False, index=None)

def dataset_p1_p():
    tid = []
    cid = []
    total = []
    num = []
    desc = []
    pdesc = ['This item is perfect','This item is awful','This item is good','This item is not bad','This item is great','This is what I want']

    n = 500
    for i in range(n):
        tid.append(i+1)
        cid.append(random.randint(1, 50))
        total.append(random.uniform(10, 2000))
        num.append(random.randint(1, 15))
        desc.append(pdesc[random.randint(0, len(pdesc)-1)])

    print(i+1, 'trans-records')

    d2 = pd.DataFrame({
        'TransID': tid,
        'CustID': cid,
        'TransTotal': total,
        'TransNumItems': num,
        'TransDesc': desc
    })
    d2.to_csv('./Purchases.csv', header=False, index=None)

if __name__=='__main__':
    dataset_p1_c()
    dataset_p1_p()
