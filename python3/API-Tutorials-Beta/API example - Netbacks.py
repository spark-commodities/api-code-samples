

# # Python API Example - Netbacks Data Import
# ## Importing Netbacks Data into a Pandas DataFrame
# 
# Here we import Netbacks data from the Spark Python API. 
# 
# We then store in a DataFrame for easy exploration and filtering.
# 
# This guide is designed to provide an example of how to access the Spark API:
# - The path to your client credentials is the only input needed to run this script
# - This script has been designed to display the raw outputs of requests from the API, and how to format those outputs to enable easy reading and analysis
# - This script can be copy and pasted by customers for quick use of the API
# - Once comfortable with the process, you can change the variables that are called to produce your own custom analysis products. (Section 2 onwards in this guide).
# 
# __N.B. This guide is just for Netbacks data. If you're looking for other API data products (such as Price releases or Freight routes), please refer to their according code example files.__ 

#%%# ## 1. Importing Data
# 
# Here we define the functions that allow us to retrieve the valid credentials to access the Spark API.
# 
# __This section can remain unchanged for most Spark API users.__



import json
import os
import sys
import numpy as np
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
        "scopes": "read:netbacks,read:access,read:prices,read:routes"
    }

    content = do_api_post_query(uri="/oauth/token/", body=body, headers=headers)

    print(
        ">>>> Successfully fetched an access token {}****, valid {} seconds.".format(
            content["accessToken"][:5], content["expiresIn"]
        )
    )

    return content["accessToken"]




#%%# ## Defining Fetch Request
# 
# Here is where we define what type of data we want to fetch from the API.
# 
# In the fetch request, we use the URL extension:
# 
# uri="/v1.0/netbacks/reference-data/"
# 
# This query shows an overview on all available netbacks, showing all available ports and possible routes to/from these destinations (i.e. via Suez, Panama etc.).
# 
# An example of pulling all available Netbacks data for a specific route (e.g. Netbacks data for Sabine Pass via Suez) is shown later in this script. 
# 
# Pulling other data products (such as price releases) require different URL's in the fetch request (refer to other Python API examples).



def list_netbacks(access_token):
    """
    Fetch available routes. Return contract ticker symbols

    # Procedure:

    Do a GET query to /v1.0/routes/ with a Bearer token authorization HTTP header.
    """
    content = do_api_get_query(uri="/v1.0/netbacks/reference-data/", access_token=access_token)

    print(">>>> All the routes you can fetch")
    tickers = []
    fobPort_names = []

    availablevia = []
    
    for contract in content["data"]['staticData']['fobPorts']:
        
        tickers.append(contract["uuid"])
        fobPort_names.append(contract['name'])
        
        availablevia.append(contract['availableViaPoints'])
    
    
    
    reldates = content["data"]['staticData']['sparkReleases']

    dicto1 = content["data"]
        
    return tickers, fobPort_names, availablevia, reldates, dicto1



#%%# ## N.B. Credentials
# 
# Here we call the above functions, and input the file path to our credentials.
# 
# N.B. You must have downloaded your client credentials CSV file before proceeding. Please refer to the API documentation if you have not dowloaded them already.
# 



# Input the path to your client credentials here
client_id, client_secret = retrieve_credentials(file_path="/Users/qasimafghan/Downloads/client_credentials.csv")

# Authenticate:
access_token = get_access_token(client_id, client_secret)
print(access_token)

# Fetch all contracts:
tickers, fobPort_names, availablevia, reldates, dicto1 = list_netbacks(access_token)


#%%

# Prints the available release dates
print(reldates)


#%%

# Prints the available Route ID numbers
print(tickers)


#%%

# Prints the available route options, corresponding to each Route ID number shown above
# I.e. availablevia[2] shows the available route options for tickers[2]
# Any empty entries in 'availablevia' means this route does not have Netbacks data to call

print(availablevia)


#%%

# Print the names of each of the ports
print(fobPort_names)


#%%

# Shows the structure of the raw dictionary called
dicto1


#%%# ### Reformatting
# 
# For a more accessible data format, we filter the data to only retrieve ports that have available Netbacks data. We then reformat this into a DataFrame.



## Check which routes have non-empty Netbacks data and save indices
via_index = []
c = 0
for a in availablevia:
    if len(a) != 0:
        via_index.append(c)
    
    c+=1

# Use these indices to retrive the corresponding Netbacks info
port_list = []
tick_list = []
via_list = []
for v in via_index:
    port_list.append(fobPort_names[v])
    tick_list.append(tickers[v])
    via_list.append(availablevia[v])
    
# Show available Netbacks ports in a DataFrame (with corresponding indices)
available_df = pd.DataFrame({
    'Index': via_index,
    'Callable Ports': port_list,
    'Corresponding Ticker': tick_list,
    'Available Via': via_list
})

available_df



#%%# ## Fetching Netbacks Data specific to one port
# 
# Now that we can see all the available Netbacks data available to us, we can start to define what ports we want to call Netbacks data for (by referring to 'available_df' above).
# 
# The first step is to choose which port ID ('my_ticker') and which price release date ('my_release') we want. We check what possible routes are available for this port ('possible_via') and then choose one ('my_via').
# 
# __This is where you should input the specific Netbacks parameters you want to see__



# Choose route ID and price release date

# Here we define which port we want
port = 'Cove Point'

ti = int(available_df[available_df['Callable Ports'] == port]['Index'])
my_ticker = tickers[ti]
my_release = reldates[0]

print(my_ticker)
print(my_release)


#%%

# See possible route passage options
possible_via = availablevia[tickers.index(my_ticker)]
print(possible_via)


#%%

# Choose route passage
my_via = possible_via[0]
print(my_via)


#%%# ## Defining the Function
# 
# We now define the function needed to call this data.
# 
# Again, we define what type of data we want to fetch from the API.
# 
# In the fetch request, we use the URL:
# 
# __uri="/v1.0/netbacks/"__
# 
# We then print the output.
# 
# __This function does not need to be altered by the user.__



## Defining the function

def fetch_netback(access_token, ticker, release, via):
    """
    For a route, fetch then display the route details

    # Procedure:

    Do GET queries to https://api.sparkcommodities.com/v1.0/routes/{route_uuid}/
    with a Bearer token authorization HTTP header.
    """
    
    
    query_params = "?fob-port={}".format(ticker)
    if release is not None:
        query_params += "&release-date={}".format(release)
    if via is not None:
        query_params += "&via-point={}".format(via)
    
    
    content = do_api_get_query(
        #uri="/v1.0/netbacks/{}/{}".format(ticker, query_params)
        #uri="/v1.0/netbacks/{}".format(query),
        uri="/v1.0/netbacks/{}".format(query_params),
        access_token=access_token,
    )
    
    my_dict = content['data']

    return my_dict


#%%## Calling that function and storing the output

# Here we store the entire dataset called from the API


my_dict = fetch_netback(access_token, my_ticker, release=my_release, via=my_via)


# Displaying the raw output
#my_dict


#%%# ## Formatting and Plotting
# 
# Here we format the raw output into several lists into a Pandas DataFrame. We then plot this data to recreate the Price Chart seen on the 'Netbacks' page on the Spark Platform.



# Storing the relevant information in lists
route_name = my_dict['name']

months = []
nea_outrights = []
nea_ttfbasis = []
nwe_outrights = []
nwe_ttfbasis = []
delta_outrights = []
delta_ttfbasis = []

for m in my_dict['netbacks']:
    
    months.append(m['load']['month'])
    
    nea_outrights.append(float(m['nea']['outright']['usdPerMMBtu']))
    nea_ttfbasis.append(float(m['nea']['ttfBasis']['usdPerMMBtu']))
    
    nwe_outrights.append(float(m['nwe']['outright']['usdPerMMBtu']))
    nwe_ttfbasis.append(float(m['nwe']['ttfBasis']['usdPerMMBtu']))
    
    delta_outrights.append(float(m['neaMinusNwe']['outright']['usdPerMMBtu']))
    delta_ttfbasis.append(float(m['neaMinusNwe']['ttfBasis']['usdPerMMBtu']))




#%%

# Store information in a DataFrame

netbacks_df = pd.DataFrame({
    'Period': months,
    'NEA Outrights': nea_outrights,
    'NWE Outrights': nwe_outrights,
    'NEA TTF Basis': nea_ttfbasis,
    'NWE TTF Basis': nwe_ttfbasis,
    'NEA-NWE Outrights': delta_outrights,
    'NEA-NWE TTF Basis': delta_ttfbasis
})

print(netbacks_df)


#%%

## Plotting, showing the NEA and NWE outrights (Top plot) and the NEA-NWE Outrights (Bottom plot)

import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

sns.set_style()
sns.set_theme(style="darkgrid")

fig, ax = plt.subplots(2, figsize=(15,9))
ax[0].set_title(route_name)
plt.xlabel('Period')
ax[0].set_ylabel('$/MMBtu')
ax[1].set_ylabel('$/MMBtu')

ax[0].plot(netbacks_df['Period'], netbacks_df['NWE Outrights'])
ax[0].plot(netbacks_df['Period'], netbacks_df['NEA Outrights'])

ax[0].legend(['NWE', 'NEA via Suez'])

ax[1].bar(netbacks_df['Period'], netbacks_df['NEA-NWE Outrights'])


#%%# ## Analysis Example 2
# 
# As another example, we compare the Sabine NEA netbacks via COGH, Suez and Panama and Sabine NWE netbacks.
# 
# 1. We fetch the Sabine netbacks via COGH, Suez and Panama separately, creating 3 dictionaries
# 2. We store store the NEA netbacks from each of these dictionaries as separate lists.
# 3. The NWE netbacks can be called from any of these 3 dictionaries, as 'via' does not affect NWE
# 4. Store all relevant info in a DataFrame
# 5. Plot accordingly



# Choose route ID and price release date

# Here we define which port we want
port2 = 'Sabine Pass'

ti2 = int(available_df[available_df['Callable Ports'] == port2]['Index'])
my_ticker2 = tickers[ti2]
my_release2 = reldates[0]
print(my_ticker2)
print(my_release2)

# See possible route passage options
possible_via2 = availablevia[tickers.index(my_ticker2)]
print(possible_via2)


#%%

# Storing all netbacks data for all 3 'via' options
cogh_dict = fetch_netback(access_token, my_ticker2, release=my_release2, via=possible_via2[0])
panama_dict = fetch_netback(access_token, my_ticker2, release=my_release2, via=possible_via2[1])
suez_dict = fetch_netback(access_token, my_ticker2, release=my_release2, via=possible_via2[2])

route_name2 = cogh_dict['name']


#%%

# Calling relevant data and storing as lists
months = []

cogh_nea_outs = []
cogh_nea_ttfb = []

panama_nea_outs = []
panama_nea_ttfb = []

suez_nea_outs = []
suez_nea_ttfb = []

nwe_outs = []
nwe_ttfb = []

for m in cogh_dict['netbacks']:
    months.append(m['load']['month'])
    cogh_nea_outs.append(float(m['nea']['outright']['usdPerMMBtu']))
    cogh_nea_ttfb.append(float(m['nea']['ttfBasis']['usdPerMMBtu']))
    
    nwe_outs.append(float(m['nwe']['outright']['usdPerMMBtu']))
    nwe_ttfb.append(float(m['nwe']['ttfBasis']['usdPerMMBtu']))

for m in panama_dict['netbacks']:
    panama_nea_outs.append(float(m['nea']['outright']['usdPerMMBtu']))
    panama_nea_ttfb.append(float(m['nea']['ttfBasis']['usdPerMMBtu']))

for m in suez_dict['netbacks']:
    suez_nea_outs.append(float(m['nea']['outright']['usdPerMMBtu']))
    suez_nea_ttfb.append(float(m['nea']['ttfBasis']['usdPerMMBtu']))


#%%

#Storing relevant info in a DataFrame

sabine_outs = pd.DataFrame({
    'Period': months,
    'NEA Outrights - COGH': cogh_nea_outs,
    'NEA Outrights - Panama': panama_nea_outs,
    'NEA Outrights - Suez': suez_nea_outs,
    'NWE Outrights': nwe_outs
})




#%%

#Plotting

fig2, ax2 = plt.subplots(figsize=(15,9))

ax2.set_title(route_name + ' Netbacks')

ax2.plot(months, sabine_outs['NEA Outrights - COGH'])
ax2.plot(months, sabine_outs['NEA Outrights - Panama'])
ax2.plot(months, sabine_outs['NEA Outrights - Suez'])
ax2.plot(months, sabine_outs['NWE Outrights'])

plt.xlabel('Period')
ax2.set_ylabel('$/MMBtu')
ax2.legend(['COGH', 'Panama', 'Suez', 'NWE'])

