from datetime import datetime, timedelta
import random
from faker import Faker
import pandas as pd
import csv

fake = Faker()

STATUS = ["authorized", "posted"]
cust_dataset = pd.read_csv(r"C:\dhika\Purwadhika\final_project\buram\anz_dataset_customer.csv")
merc_dataset = pd.read_csv(r"C:\dhika\Purwadhika\final_project\buram\anz_dataset_merchant.csv")
loc_dataset = pd.read_csv(r"C:\dhika\Purwadhika\final_project\buram\location.csv")

def generate_dummy_anz_data():
    status =  random.choice(["authorized", "posted"])
    card_present_flag = random.choice(["0", "1"]) if status == "authorized" else None
    bpay_biller_code = random.choice(["0", None]) if status == "posted" else None
    first_name = random.choice(list(cust_dataset["first_name"]))
    account = cust_dataset.loc[cust_dataset["first_name"] == first_name, "account"].iloc[0]
    currency = "AUD"
    long_lat = cust_dataset.loc[cust_dataset["first_name"] == first_name, "long_lat"].iloc[0]
    txn_description = "PAY/SALARY" if bpay_biller_code == "0" else random.choice(["POS", "SALES-POS"]) if status == "authorized" else random.choice(["INTER BANK", "PAYMENT", "PHONE BANK"])
    merchant_id = random.choice(list(merc_dataset["merchant_id"]))
    merchant_code = "0" if bpay_biller_code == "0" else None
    # balance = random.gauss(14704.19, 31503.72)
    fake_date = fake.date_time_between(start_date = datetime.now() - timedelta(hours=24))
    extraction = fake_date.isoformat()
    date = fake_date.date().isoformat()
    gender = random.choice(["M", "F"])
    age = cust_dataset.loc[cust_dataset["first_name"] == first_name, "age"].iloc[0]
    merchant_state = random.choice(list(loc_dataset["merchant_state"]))
    merchant_suburb = random.choice(list(loc_dataset.loc[loc_dataset["merchant_state"] == merchant_state]))
    amount = round(random.gauss(1898.72, 5),2) if txn_description == "PAY/SALARY" else round(random.gauss(52.57, 5), 2)
    transaction_id = fake.uuid4()
    country = "Australia"
    customer_id = cust_dataset.loc[cust_dataset["first_name"] == first_name, "customer_id"].iloc[0]
    merchant_long_lat = merc_dataset.loc[merc_dataset["merchant_id"] == merchant_id, "merchant_long_lat"].iloc[0]
    movement = "credit" if txn_description == "PAY/SALARY" else "debit"

    
    return{
        "status" : status,
        "card_present_flag" : card_present_flag,
        "bpay_biller_code" : bpay_biller_code,
        "account" : account,
        "currency" : currency,
        "long_lat" : long_lat,
        "txn_description" : txn_description,
        "merchant_id" : merchant_id,
        "merchant_code" : merchant_code,
        "first_name" : first_name,
        "date" : date,
        "gender" : gender,
        "age" : age,
        "merchant_suburb" : merchant_suburb,
        "merchant_state" : merchant_state,
        "extraction" : extraction,
        "amount" : amount,
        "transaction_id" : transaction_id,
        "country" : country,
        "customer_id" : customer_id,
        "merchant_long_lat" : merchant_long_lat,
        "movement" : movement
    }

if __name__ == "__main__":
    
    data = []

    for num in range(random.randint(50_000, 100_000)):
        data.append(generate_dummy_anz_data())

    fieldnames = list(generate_dummy_anz_data().keys())
    csv_filename = f"data_source_{datetime.today().year}-{datetime.today().month}-{datetime.today().day}.csv"

    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)