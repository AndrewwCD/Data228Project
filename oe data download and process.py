import os
import urllib.request
import csv

url = 'https://download.bls.gov/pub/time.series/oe/'

# Create a directory to store the downloaded files
if not os.path.exists('bls_files'):
    os.mkdir('bls_files')

# Download all the .txt files and convert them to .csv
for filename in urllib.request.urlopen(url).read().decode().split('\n'):
    if filename.endswith('.txt'):
        file_url = url + filename
        urllib.request.urlretrieve(file_url, 'bls_files/' + filename)
        with open('bls_files/' + filename, 'r') as txt_file:
            with open('bls_files/' + os.path.splitext(filename)[0] + '.csv', 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                for line in txt_file:
                    row = line.strip().split('\t')
                    csv_writer.writerow(row)

# Read the CSV file
file_path = 'oe.area.csv'
df = pd.read_csv(file_path)

# Create new columns for latitude and longitude
df['latitude'] = None
df['longitude'] = None

# Initialize the geolocator
geolocator = Nominatim(user_agent="city_coordinates")

# Function to handle GeocoderTimedOut exception
def get_coordinates(area_name, geolocator):
    try:
        location = geolocator.geocode(area_name)
        return location
    except GeocoderTimedOut:
        return get_coordinates(area_name, geolocator)

# Populate the latitude and longitude columns
number_found = 0
for index, row in df.iterrows():
    area_name = row['area_name']
    print(area_name)
    try:
        location = get_coordinates(area_name, geolocator)
        time.sleep(1.5)
    except:
        time.sleep(5)
        try:
            location = get_coordinates(area_name, geolocator)
        except:
            pass
    if location is not None:
        print("found coordinates")
        number_found = number_found + 1
        print(number_found)
        df.at[index, 'latitude'] = location.latitude
        df.at[index, 'longitude'] = location.longitude

# Save the updated DataFrame to a new CSV file
df.to_csv('oe.area.csv', index=False)


