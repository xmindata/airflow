import requests

def collect_data(file_path, url=None):
    # Send a GET request to the URL
    if not url: 
        print ('No URL provided, make sure you already have the data in the correct folder')
        return
    
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Write the data to a file
        with open(file_path, 'w') as f:
            f.write(response.text)
    else:
        print(f'Failed to retrieve data: {response.status_code}')