import pandas as pd
import json
import sqlite3

print("Loading files...")
orders_df = pd.read_csv('orders.csv')

with open('users.json', 'r') as f:
    users_data = json.load(f)
users_df = pd.DataFrame(users_data)

conn = sqlite3.connect(':memory:')
with open('restaurants.sql', 'r') as f:
    sql_script = f.read()
conn.executescript(sql_script)
restaurants_df = pd.read_sql_query("SELECT * FROM restaurants", conn)
print("Merging data...")

merged_df = orders_df.merge(users_df, on='user_id', how='left')
final_df = merged_df.merge(restaurants_df, on='restaurant_id', how='left')

output_filename = 'final_food_delivery_dataset.csv'
final_df.to_csv(output_filename, index=False)

print(f"SUCCESS! Created: {output_filename}")

print(f"Total rows: {len(final_df)}")
