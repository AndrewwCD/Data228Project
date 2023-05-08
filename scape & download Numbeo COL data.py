#import libraries
import os
import numpy as np
import csv
import time
import copy
import requests
from bs4 import BeautifulSoup
import random

def wait():
    time.sleep(0.5 + random.random())

def scrape_numbeo_city_data(city, state):
    base_url = 'https://www.numbeo.com/cost-of-living/in/'

    url = base_url + city + "-" + state + "-United-States"
    
    wait()
    response = requests.get(url)
    if response.status_code == 200:
        print("successfull")
        soup = BeautifulSoup(response.content, 'html.parser')
        page_text = str(soup)

        if "Cannot find city id" in page_text:

            url = base_url + city + "-" + state
            wait()
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = str(soup)
            if "Cannot find city id" in page_text:

                url = base_url + city
                wait()
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')
                page_text = str(soup)
                if "Cannot find city id" in page_text:
                    print("Can't find city id. Try again")

                else:
                    print("Found city id")
                    return soup
                
            else:
                print("Found city id")
                return soup

        else:
            print("Found city id")
            return soup

    else:
        print("Failed to retrieve the webpage")

def find_data_table(tables):
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            for cell in cells:

                if "Meal, Inexpensive Restaurant" in cell.text.strip():
                    return table
                else:
                    continue

def clean_data_rows(rows):
    for row in rows:
            cells = row.find_all("td")

            price = "NULL"
            price_range = "NULL-NULL"
            price_range_min = "NULL"
            price_range_max = "NULL"
            counter = 0
            for cell in cells:
                if counter == 0:
                    item = cell.text.strip()
                    price = "NULL"
                    price_range = "NULL-NULL"
                if counter == 1:
                    price = cell.text.strip()
                    price_range = "NULL-NULL"
                if counter == 2:
                    price_range = cell.text.strip()
                counter = counter + 1
            try:
                if item == prev_item:
                        continue
                price = price.replace(",","")
                price = price.replace("$","")
                price = price.replace(" ","")
                price = price.replace("?","NULL")
                price_range = price_range.replace(",","")
                price_range = price_range.replace("$","")
                price_range = price_range.replace(" ","")
                if price_range == "":
                    price_range_min, price_range_max = "NULL", "NULL"
                else:
                    price_range = price_range.split("-")
                    price_range_min = price_range[0]
                    price_range_max = price_range[1]

                writer.writerow([city, state, item, price, price_range_min, price_range_max])
                prev_item = item
            except Exception as e:
                print(e)
                pass
        return rows

url = 'https://www.numbeo.com/cost-of-living/country_result.jsp?country=United+States'
response = requests.get(url)

if response.status_code == 200:  # Check if the webpage was successfully retrieved

    soup = BeautifulSoup(response.content, 'html.parser')
    dropdown = soup.find('select', {'name': 'city'})  
    options = dropdown.find_all('option')
    cities_states = [option['value'] for option in options]  
    print(cities_states)
    
else:
    print('Failed to retrieve the webpage')

#For Numbeo cost of living data of each city in the US, the links to each city's data
#Follows the following formats: https://www.numbeo.com/cost-of-living/in +
#1) /city_name (if the city doesn't exist in any other US state or country)
#2) /city_name-state_abbraviation (if the state of the city doesn't exist in any other country)
#3) /city_name-state_abbreviation-United-States (if the state exists in another country)
#For scraping, first try #3, then #2, then #1. Otherwise it is possible to get data for the city outside of US.

with open("Numbeo US cities cost of living data.csv", mode = "w", newline = "") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["City", "State", "Item", "Price", "Price Range Min", "Price Range Max"])
    prev_item = ""
    city_number = 0
    omitted_cities = []
    for i in range(1,len(cities_states)):
        city_number = city_number+1
        city_state = cities_states[i].replace(",","")
        city_state =  city_state.split(" ")
        if len(city_state) > 2:
            city = city_state[0]
            for list_index in range(1,len(city_state)-1):
                city = city + "-" + city_state[list_index]
        else:
            city = city_state[0].strip()
        state = city_state[-1]
        
        soup = scrape_numbeo_city_data(city, state)
        
        try:
            tables = soup.find_all("table")
        except:
            omitted_cities.append(cities_states[i])
            continue
            
        data_table = find_data_table(tables)
        rows = data_table.find_all("tr")

        rows = clean_data_rows(rows)
            

with open("Omitted Cities.csv", mode = "w", newline = "") as csv_file:
    writer = csv.writer(csv_file)
    for i in range(len(omitted_cities)):
        writer.writerow(omitted_cities[i])
    

            
    
