"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
|                                                                      |
|  Date: 10/05/2024                             Author: Vineet Saraf   |
|  ----------------                             --------------------   |
|  File:                                                               |
|  grabdata.py - This file scrapes data from a USAToday website and    |
|                assembles a CSV out of it.                            |
|                                                                      |
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import json
import requests
import re
from datetime import datetime
import pandas as pd
from dateutil import parser
from bs4 import BeautifulSoup
import os
import time

OUTPUT_FILE = './outagedata/outputnew2.csv'
WEB_PREFIX = 'https://data.usatoday.com/national-power-outage-map-tracker/area/'

def grabdata(url):
    # Send a GET request to fetch the content of the page
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
    try:
        response = requests.get(url, headers=headers)
    except Exception as e:
        print(f"ERROR: {e}")
        return

    # Initialize a dictionary to store county names and their corresponding numbers
    county_dict = {}

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the content with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all <a> tags on the page
        a_tags = soup.find_all('a')

        # Iterate through each <a> tag
        for a in a_tags:
            href = a.get('href')

            # Check if href matches the expected URL format
            pattern = r'\/area\/([a-z\-]+)\/(\d+)\/$'

            # Search for matches
            match = re.search(pattern, href)

            if match:
                county_name = match.group(1).replace('-', ' ').title()  # First capture group is the county name
                number = match.group(2) # Second capture group is the number

                if len(number) < 5: 
                    continue

                # Create a dictionary relating county name and number
                county_dict[number] = county_name


        # Print the resulting dictionary
        print(county_dict)

        """
        DEBUG CODE DELETE LATER
        """
        keys = list(county_dict.keys())
        if '21041' in keys:
            bindex = keys.index('21041')  # '13027' corresponds to Brooks County Ga
            county_dict = {key: county_dict[key] for key in keys[bindex:]}

    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")

    # Setup Chrome options
    chrome_options = Options()
    
    chrome_options.add_argument('--headless')  # Run in headless mode
    chrome_options.add_argument('--no-sandbox')
    
    chrome_options.add_argument("--start-minimized")  # Start Chrome minimized
    chrome_options.add_argument("--auto-open-devtools-for-tabs")
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    def get_county_outages(countyid: str):
        # Initialize the WebDriver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.set_window_position(-2400, 0) # render off screen 

        # Define a listener for network events using Chrome DevTools Protocol
        def intercept_network_traffic(driver):
            # Get the DevTools protocol and enable network monitoring
            dev_tools = driver.execute_cdp_cmd('Network.enable', {})
            response = None

            # Listen for network events
            def request_listener(event):
                nonlocal response
                # The event we're interested in is the 'Network.responseReceived'
                if 'params' in event and 'response' in event['params']:
                    url = event['params']['response']['url']
                    if url.endswith(countyid + "/"):  
                        file_url = url
                        print(f"File found: {url}")

                        max_retries = 30
                        retry_count = 0

                        while retry_count < max_retries:
                            try:
                                # Fetch the response content directly
                                response = requests.get(file_url, timeout=15)
                                response.raise_for_status()
                                break  # Exit loop if request is successful
                            except requests.exceptions.Timeout:
                                retry_count += 1
                                print(f"Timeout error. Retrying {retry_count}/{max_retries}...")
                                time.sleep(5)  # Wait before retrying
                            except requests.exceptions.RequestException as e:
                                print(f"An error occurred: {e}")
                                time.sleep(5)  # Wait before retrying
                        else:
                            print("Max retries exceeded. The request failed.")
                        """
                        if response.status_code == 200:
                            print(f"Response content for {file_url}:\n{response.content.decode('utf-8')}")
                        else:
                            print(f"Failed to get content from {file_url}, status code: {response.status_code}")
                        """
            
            # Attach the event listener to all network traffic
            driver.execute_script(
                "window.addEventListener('message', function(event) { return event.data; })"
            )

            # Capture network traffic
            logs = driver.get_log("performance")
            for entry in logs:
                event = json.loads(entry["message"])["message"]
                if event["method"] == "Network.responseReceived":
                    request_listener(event)

            return response.text
        
        urlized_name = county_dict[countyid].replace(" ", "-").lower()

        print("alpha")
        # Open the website where the file is downloaded
        driver.get(f"{WEB_PREFIX}{urlized_name}/{countyid}/")  # Replace with the URL you're monitoring
        print("beta")

        # Intercept and check for the file
        response = intercept_network_traffic(driver)

        driver.implicitly_wait(15) 

        # Close the browser
        driver.quit()

        # Regex pattern to match the barChartData object
        regex = r"var barChartData = \{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}"

        # Perform the regex search
        match = re.search(regex, response)
        """with open('./test.txt', 'w') as file:
            file.write(response)"""
        if match:
            # Extract the matched string
            bar_chart_data_str = match.group(0)
            
            # Remove the 'var barChartData = ' part
            bar_chart_data_str = bar_chart_data_str.split('=', 1)[1].strip()
            
            # Convert to JSON
            #bar_chart_data_str = bar_chart_data_str.rstrip(';')  # Remove the trailing semicolon
            #bar_chart_data_json = json.loads(bar_chart_data_str)

            # Replace single quotes with double quotes
            valid_json_str = bar_chart_data_str.replace("'", '"').rstrip(';')

            # Load the string into a JSON object
            json_data = json.loads(valid_json_str)

            with open('./outagedata/output.json', 'w') as json_file:
                json.dump(json_data, json_file, indent=4)

        else:
            print(f"No match found. {urlized_name}")
            return

        # Extract labels and data from the JSON structure
        labels = json_data["labels"]
        data_values = json_data["datasets"][0]["data"]

        # Convert labels to YYYY-MM-DD HH:00 format
        dates = []
        for label in labels:
            # Use dateutil's parser to handle the date
            label = re.sub(r'Sept\.', 'Sep', label).replace("a.m.", "AM").replace("p.m.", "PM").replace(".", "")
            date_obj = datetime.strptime(label, '%b %d, %I %p')

            # Format the date to the desired output
            new_date = date_obj.strftime('2024-%m-%d %H:00')

            # Fixing a strange bug in the data where this time is duplicated
            if ('2024-09-26 20:00' in new_date): 
                if ('2024-09-26 19:00' not in dates):
                    new_date = '2024-09-26 19:00'

            # Fixing a strange bug in the data where this time is duplicated
            if ('2024-09-26 23:00' in new_date): 
                if ('2024-09-26 22:00' not in dates):
                    new_date = '2024-09-26 22:00'

            dates.append(new_date)


        if os.path.exists(OUTPUT_FILE):
            # Read the CSV into a DataFrame
            df = pd.read_csv(OUTPUT_FILE)
            
            # Check if the column already exists
            column_name = county_dict[countyid]

            if column_name not in df.columns:
                # Add the new column if it doesn't exist
                new_data = {
                    'Dates': dates,  # Replace with your date data
                    county_dict[countyid]: [int(x) for x in data_values] # Replace with your new data values
                }

                new_df = pd.DataFrame(new_data)
                df = pd.merge(df, new_df, on='Dates', how='outer')

                # Save the DataFrame back to the CSV (overwrite)
                df.to_csv(OUTPUT_FILE, index=False)
                print(f"Column '{column_name}' added successfully!")
            else:
                print(f"Column '{column_name}' already exists!")
        else:
            # Create a DataFrame
            df = pd.DataFrame({
                "Dates": dates,
                county_dict[countyid]: [int(x) for x in data_values]
            })

            # Save the DataFrame to a CSV file
            df.to_csv(OUTPUT_FILE, index=False)

        # Ensure data is good
        df = pd.read_csv(OUTPUT_FILE)
        df['Dates'] = pd.to_datetime(df['Dates'])
        all_unique = df['Dates'].is_unique


        if not all_unique:
            non_unique_rows = df[df.duplicated('Dates', keep=False)]
            print(non_unique_rows)
            exit(0)
        
    for id, _ in county_dict.items():
        get_county_outages(id)

"""
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/south-carolina/45/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/georgia/13/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/florida/12/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/tennessee/47/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/virginia/51/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/kentucky/21/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/ohio/39/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/west-virginia/54/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/alabama/01/')
#grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/north-carolina/37/')
"""

grabdata('https://data.usatoday.com/national-power-outage-map-tracker/area/south-carolina/45/')
