#import libraries
import os
import numpy as np
import pandas as pd
import csv

oe_area = pd.read_csv(r"D:\Data 228\Data 228 Project\BLS "
                      r"Occupation Employment and Wage Statistics\oe.area.csv", dtype=str)

numbeo_data = pd.read_csv(r"D:\Data 228\Data 228 Project\Numbeo Data"
                          r"\Numbeo US cities cost of living data.csv", dtype=str)
numbeo_data = numbeo_data.replace(np.nan, "NULL", regex = True)

# Add new column to numbeo data with area_code primary key
numbeo_data["area_code"] = np.nan

# Convert all city names to lowercase and remove spaces and dashes
oe_area[['city', 'state']] = oe_area['area_name'].str.split(', ', expand=True)

# Find the max area_code value in oe_area. New area_code values will then be higher than the max value.
area_code_max = oe_area["area_code"].max()

# Find unique city-state pairs in numbeo_data
unique_cities = numbeo_data[['city', 'state']].drop_duplicates()

# Check for matches in oe_area
print("matching cities")
matching_cities = []
for numbeo_index, numbeo_row in unique_cities.iterrows():
    numbeo_city = numbeo_row["city"]
    numbeo_state = numbeo_row["state"]
    for oe_index, oe_row, in oe_area.iterrows():
        try:
            oe_area_city = oe_row["city"] 
            oe_area_state = oe_row["state"] #consider only first two characteres
            oe_area_code = oe_row["area_code"]
            if numbeo_city in oe_area_city and numbeo_state in oe_area_state:
                matching_cities.append([oe_area_city,oe_area_state,oe_area_code])
                rows_to_update = numbeo_data.loc[(numbeo_data['city'] == numbeo_city) & (numbeo_data['state'] == numbeo_state)]
                for index, row in rows_to_update.iterrows():
                    numbeo_data.at[index, 'area_code'] = oe_area_code
                #numbeo_row["area_code"] = oe_area_code
        except:
            pass

#Drop rows with null values in numbeo_data
numbeo_data = numbeo_data.dropna(subset=['area_code'])
numbeo_data = numbeo_data.replace(np.nan, "NULL", regex = True

numbeo_data.to_csv('Numbeo Data Reformatted.csv', encoding='utf-8-sig', index=False)

numbeo_data["price"] = numbeo_data["price"].str.strip()
numbeo_data["price_range_max"] = numbeo_data["price_range_max"].str.strip()
numbeo_data["price_range_min"] = numbeo_data["price_range_min"].str.strip()

# Create a new dataframe with unique item_names and corresponding item_names
item_names = numbeo_data['item_name'].unique()
cost_of_living_item = pd.DataFrame({'item_name': item_names})

# Create a new column for item_code with unique integer values
cost_of_living_item['item_code'] = pd.factorize(item_names)[0]

# Merge the new item_df with the numbeo_data dataframe
merged_df = pd.merge(numbeo_data, cost_of_living_item, on='item_name', how='left')

# Print merged_df to confirm the new item_code column
print(merged_df)

merged_df.drop("item_name", axis=1, inplace=True)
merged_df.drop("city", axis=1, inplace=True)
merged_df.drop("state", axis=1, inplace=True)

merged_df.to_csv('cost of living price.csv', encoding='utf-8-sig', index=False)
cost_of_living_item.to_csv('cost of living item.csv', encoding='utf-8-sig', index=False)
