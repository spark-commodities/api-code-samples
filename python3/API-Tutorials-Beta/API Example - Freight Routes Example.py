
# # Python API Example - Freight Route Data Import
# ## Importing Route Data into a Pandas DataFrame
# 
# Here we import shipping route data from the Spark Python API. 
# 
# We then store in a DataFrame for easy exploration and filtering.
# 
# This guide is designed to provide an example of how to access the Spark API:
# - The path to your client credentials is the only input needed to run this script
# - This script has been designed to display the raw outputs of requests from the API, and how to format those outputs to enable easy reading and analysis
# - This script can be copy and pasted by customers for quick use of the API
# - Once comfortable with the process, you can change the variables that are called to produce your own custom analysis products. (Section 2 onwards in this guide).
# 
# __N.B. This guide is just for Freight route data. If you're looking for other API data products (such as Price releases or Netbacks), please refer to their according code example files.__ 


#%%


# ## 1. Importing Data
# 
# Here we define the functions that allow us to retrieve the valid credentials to access the Spark API.
# 
# This section can remain unchanged for most Spark API users.



import json
import os
import sys
import pandas as pd
from base64 import b64encode
from pprint import pprint
from urllib.parse import urljoin


try:
    from urllib import request, parse
    from urllib.error import HTTPError
except ImportError:
    raise RuntimeError("Python 3 required")


API_BASE_URL = "https://api.sparkcommodities.com"


def retrieve_credentials(file_path=None):
    """
    Find credentials either by reading the client_credentials file or reading
    environment variables
    """
    if file_path is None:

        client_id = os.getenv("SPARK_CLIENT_ID")
        client_secret = os.getenv("SPARK_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError(
                "SPARK_CLIENT_ID and SPARK_CLIENT_SECRET environment vars required"
            )
    else:
        # Parse the file
        if not os.path.isfile(file_path):
            raise RuntimeError("The file {} doesn't exist".format(file_path))

        with open(file_path) as fp:
            lines = [l.replace("\n", "") for l in fp.readlines()]

        if lines[0] in ("clientId,clientSecret", "client_id,client_secret"):
            client_id, client_secret = lines[1].split(",")
        else:
            print("First line read: '{}'".format(lines[0]))
            raise RuntimeError(
                "The specified file {} doesn't look like to be a Spark API client "
                "credentials file".format(file_path)
            )

    print(">>>> Found credentials!")
    print(
        ">>>> Client_id={}, client_secret={}****".format(client_id, client_secret[:5])
    )

    return client_id, client_secret


def do_api_post_query(uri, body, headers):
    url = urljoin(API_BASE_URL, uri)

    data = json.dumps(body).encode("utf-8")

    # HTTP POST request
    req = request.Request(url, data=data, headers=headers)
    try:
        response = request.urlopen(req)
    except HTTPError as e:
        print("HTTP Error: ", e.code)
        print(e.read())
        sys.exit(1)

    resp_content = response.read()

    # The server must return HTTP 201. Raise an error if this is not the case
    assert response.status == 201, resp_content

    # The server returned a JSON response
    content = json.loads(resp_content)

    return content


def do_api_get_query(uri, access_token):
    url = urljoin(API_BASE_URL, uri)

    headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Accept": "application/json",
    }

    # HTTP POST request
    req = request.Request(url, headers=headers)
    try:
        response = request.urlopen(req)
    except HTTPError as e:
        print("HTTP Error: ", e.code)
        print(e.read())
        sys.exit(1)

    resp_content = response.read()

    # The server must return HTTP 201. Raise an error if this is not the case
    assert response.status == 200, resp_content

    # The server returned a JSON response
    content = json.loads(resp_content)

    return content


def get_access_token(client_id, client_secret):
    """
    Get a new access_token. Access tokens are the thing that applications use to make
    API requests. Access tokens must be kept confidential in storage.

    # Procedure:

    Do a POST query with `grantType` and `scopes` in the body. A basic authorization
    HTTP header is required. The "Basic" HTTP authentication scheme is defined in
    RFC 7617, which transmits credentials as `clientId:clientSecret` pairs, encoded
    using base64.
    """

    # Note: for the sake of this example, we choose to use the Python urllib from the
    # standard lib. One should consider using https://requests.readthedocs.io/

    payload = "{}:{}".format(client_id, client_secret).encode()
    headers = {
        "Authorization": b64encode(payload).decode(),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    body = {
        "grantType": "clientCredentials",
        "scopes": "read:lng-freight-prices,read:routes",
    }

    content = do_api_post_query(uri="/oauth/token/", body=body, headers=headers)

    print(
        ">>>> Successfully fetched an access token {}****, valid {} seconds.".format(
            content["accessToken"][:5], content["expiresIn"]
        )
    )

    return content["accessToken"]



#%%

# ## Defining Fetch Request
# 
# Here is where we define what type of data we want to fetch from the API.
# 
# In the fetch request, we use the URL extension:
# 
# uri="/v1.0/routes/"
# 
# This is to query shipping route data specifically. Other data products (such as price releases) require different URL's in the fetch request (refer to other Python API examples).



def list_routes(access_token):
    """
    Fetch available routes. Return route ids and Spark price release dates.

    # Procedure:

    Do a GET query to /v1.0/routes/ with a Bearer token authorization HTTP header.
    """
    content = do_api_get_query(uri="/v1.0/routes/", access_token=access_token)

    print(">>>> All the routes you can fetch")
    tickers = []
    for contract in content["data"]['routes']:
     
        tickers.append(contract["uuid"])
  
    
    reldates = content["data"]['sparkReleaseDates']
    
    dicto1 = content["data"]
        
    return tickers, reldates, dicto1


#%%

# ## N.B. Credentials
# 
# Here we call the above functions, and input the file path to our credentials.
# 
# N.B. You must have downloaded your client credentials CSV file before proceeding. Please refer to the API documentation if you have not dowloaded them already.
# 
# The code then prints:
# - the number of callable routes available
# - the number of Spark freight price dates that are callable



## Input your file location here
client_id, client_secret = retrieve_credentials(file_path="/Users/qasimafghan/Downloads/client_credentials.csv")

# Authenticate:
access_token = get_access_token(client_id, client_secret)
print(access_token)

# Fetch all contracts:
tickers, reldates, dicto1 = list_routes(access_token)


#print(reldates)

print(len(tickers))
print(len(reldates))

#%%
# # 2. Describing available routes
# 
# We have now saved all available routes as a dictionary. We can check how this looks, and then filter the routes by several characteristics.



# raw dictionary

print(dicto1)


#%%


# Store some of the route characteristics in lists, and check these lists are the same length
# N.B. these are not all the characteristics available! 
# Check the Output of the raw dictionary (above) to see all available characteristics.

primary_uuid =[]
load_uuid = []
discharge_uuid = []
load_port = []
discharge_port = []
via_list = []
load_region = []
discharge_region = []

for route in dicto1['routes']:
    primary_uuid.append(route['uuid'])
    via_list.append(route['via'])
    
    load_uuid.append(route['loadPort']['uuid'])
    load_port.append(route['loadPort']['name'])
    load_region.append(route['loadPort']['region'])
    
    discharge_uuid.append(route['dischargePort']['uuid'])
    discharge_port.append(route['dischargePort']['name'])
    discharge_region.append(route['dischargePort']['region'])

    
print(len(primary_uuid))
print(len(load_uuid))
print(len(discharge_uuid))
print(len(load_port))
print(len(discharge_port))
print(len(via_list))
print(len(load_region))
print(len(discharge_region))




#%%
# ### Storing as a DataFrame and exploring the data
# 
# Now that we've sorted and stored the routes and some of their characteristics, we can create a Pandas DataFrame to read this data easily.
# 
# The '.head()' function retreives the first 5 rows of the DataFrame, just to show an example of how the data is laid out


route_df = pd.DataFrame({
    'UUID': primary_uuid,
    'Load Location': load_port,
    'Discharge Location': discharge_port,
    'Via': via_list,
    'Load Region': load_region,
    'Discharge Region': discharge_region,
    'Load UUID': load_uuid,
    'Discharge UUID': discharge_uuid
})

route_df.head()




#%%
# #### Filtering Data
# 
# We can then filter the data based off several conditions. Two examples are shown below:
# 
# 1. Routes where the Loading location is Gate
# 2. Routes where the Loading location is Gorgon AND the route involves passage through the Suez Canal



route_df[route_df['Load Location'] == 'Gate']


#%%

route_df[(route_df['Load Location'] == 'Gorgon') & (route_df['Via'] == 'suez')]



#%%
# #### Unique values
# 
# We can also see all the ports that the listed routes originate from, and how many routes originate from each port.



print(route_df['Load Location'].unique())

route_df['Load Location'].value_counts()


#%%
# # 3. Analysing a Specific Route
# 
# 
# Here we define the function that allows us to pull data for a specific route and release date.
# 
# We then define a given route ID ('my_route') and release date ('my_release') below the function, and these values are printed out for the user to check the parameters.



## Defining the function

def fetch_route_data(access_token, ticker, release):
    """
    For a route, fetch then display the route details

    # Procedure:

    Do GET queries to https://api.sparkcommodities.com/v1.0/routes/{route_uuid}/
    with a Bearer token authorization HTTP header.
    """
    
    query_params = "?release-date={}".format(release)
    
    content = do_api_get_query(
        uri="/v1.0/routes/{}/{}".format(ticker, query_params),
        access_token=access_token,
    )
    
    my_dict = content['data']
    

    print(">>>> Get route information for {}".format(ticker))

    return my_dict




my_route = tickers[2]
my_release = reldates[2]


print(my_route)
print(my_release)




#%%

## Calling that function and storing the output

# Here we store the entire dataset called from the API

my_dict = fetch_route_data(access_token, tickers[2], release=reldates[2])


#%%

## Calling that dictionary to see how it is structured

my_dict


#%%

### Define a variable storing the route start-end
route_name = my_dict['name']


#%%# ### Storing Data as a DataFrame
# 
# We extract some relevant data for the chosen route, including the spot price and forward prices. These are stored in a DataFrame and an example Forward curve is plotted using this data.



period = []
start_list = []
end_list = []
usd = []
usdpermmbtu = []
hire_cost = []


for data in my_dict['dataPoints']:
    start_list.append(data['deliveryPeriod']['startAt'])
    end_list.append(data['deliveryPeriod']['endAt'])
    period.append(data['deliveryPeriod']['name'])
    
    usd.append(data['costsInUsd']['total'])
    usdpermmbtu.append(data['costsInUsdPerMmbtu']['total'])
    
    hire_cost.append(data['costsInUsd']['hire'])
    

    
my_route_df = pd.DataFrame({
    'Period':period,
    'Start Date': start_list,
    'End Date': end_list,
    'Cost in USD': usd,
    'Cost in USDperMMBtu': usdpermmbtu,
    'Hire Cost in USD': hire_cost
})




#%%

## Changing the data type of these columns from 'string' to numbers.
## This allows us to easily plot a forward curve, as well as perform statistical analysis on the prices.


my_route_df['Cost in USD']=pd.to_numeric(my_route_df['Cost in USD'])
my_route_df['Hire Cost in USD']=pd.to_numeric(my_route_df['Hire Cost in USD'])
my_route_df['Cost in USDperMMBtu']=pd.to_numeric(my_route_df['Cost in USDperMMBtu'])


#%% ### Plotting
# 
# Here we plot the data from the DataFrame using the matplotlib package



## Plotting

import seaborn as sns
import matplotlib.pyplot as plt

sns.set_style()
sns.set_theme(style="darkgrid")

fig, ax = plt.subplots(figsize=(15,9))
ax.scatter(my_route_df['Period'], my_route_df['Cost in USD'])
ax.plot(my_route_df['Period'], my_route_df['Cost in USD'])

ax.scatter(my_route_df['Period'], my_route_df['Hire Cost in USD'])
ax.plot(my_route_df['Period'], my_route_df['Hire Cost in USD'])

ax.set_title(route_name)
plt.xlabel('Period')
plt.ylabel('Cost in USD')


# In[ ]:




