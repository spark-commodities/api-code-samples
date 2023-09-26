
# # Python API Example - Price Release Data Import and Storage
# ## Importing Price Data into a Pandas DataFrame and Plotting
# 
# Here we import price release data from the Spark Python API. 
# 
# We then store them as local variables so that they can be used for analysis.
# 
# This guide is designed to provide an example of how to access the Spark API:
# - The path to your client credentials is the only input needed to run this script
# - This script has been designed to display the raw outputs of requests from the API, and then shows you how to format those outputs to enable easy reading and analysis
# - This script can be copied and pasted by customers for quick use of the API
# - Once comfortable with the process, you can change the variables that are called to produce your own custom analysis products. (Section 2 onwards in this guide).
# 
# __N.B. This guide is just for Price release data. If you're looking for other API data products (such as Freight routes or Netbacks), please refer to their according code example files.__ 


#%%##################

# N.B. These scripts are adapted from Jupyter Notebook files, hence the cell structure below.
##### The entire file can be run, but we recommend running each cell separately to view each cell's output.
#

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
# In my fetch request, I use the URL:
# 
# uri="/v1.0/contracts/"
# 
# This is to query contract price data specifically. Other data products (such as shipping route costs) require different URL's in the fetch request (refer to other Python API examples).



def list_contracts(access_token):
    """
    Fetch available contracts. Return contract ticker symbols

    # Procedure:

    Do a GET query to /v1.0/contracts/ with a Bearer token authorization HTTP header.
    """
    content = do_api_get_query(uri="/v1.0/contracts/", access_token=access_token)

    print(">>>> All the contracts you can fetch")
    tickers = []
    for contract in content["data"]:
        print(contract["fullName"])
        tickers.append(contract["id"])

    return tickers

#%%

# ## N.B. Credentials
# 
# Here we call the above functions, and input the file path to our credentials.
# 
# N.B. You must have downloaded your client credentials CSV file before proceeding. Please refer to the API documentation if you have not dowloaded them already.
# 
# The code then prints the available prices that are callable from the API, and their corresponding Python ticker names are displayed as a list at the bottom of the Output.



#Insert file path to your client credentials here
client_id, client_secret = retrieve_credentials(file_path="/Users/qasimafghan/Downloads/client_credentials.csv")

# Authenticate:
access_token = get_access_token(client_id, client_secret)
print(access_token)

# Fetch all contracts:
tickers = list_contracts(access_token)


print(tickers)

#%%
# ## 2. Latest Price Release
# 
# Here we call the latest price release and print it in a readable format. This is done using the URL extension:
# 
# "/v1.0/contracts/{contract_ticker_symbol}/price-releases/latest/"
# 
# 'tickers[2]' is the Python ticker called here. 'tickers' refers to the printed list above, so we can see that 'tickers[2]' refers to 'spark25s'.
# 
# We then save the entire dataset as a local variable called 'my_dict'.
# 
# N.B. The first two tickers, 'spark25f' and 'spark30f', are deprecated. Historical data for these tickers are available up until 2022-04-01 (yyyy-mm-dd)__



## Defining the function

def fetch_latest_price_releases(access_token, ticker):
    """
    For a contract, fetch then display the latest price release

    # Procedure:

    Do GET queries to /v1.0/contracts/{contract_ticker_symbol}/price-releases/latest/
    with a Bearer token authorization HTTP header.
    """
    content = do_api_get_query(
        uri="/v1.0/contracts/{}/price-releases/latest/".format(ticker),
        access_token=access_token,
    )
    
    
    release_date = content["data"]["releaseDate"]

    print(">>>> Get latest price release for {}".format(ticker))
    print("release date =", release_date)

    data_points = content["data"]["data"][0]["dataPoints"]

    for data_point in data_points:
        period_start_at = data_point["deliveryPeriod"]["startAt"]

        spark_prices = dict()
        for unit, prices in data_point["derivedPrices"].items():
            spark_prices[unit] = prices["spark"]

        print(f"Spark Price={spark_prices} for period starting on {period_start_at}")
        print(ticker)
    
    
    return content['data']


## Calling that function and storing the output

# Here we store the entire dataset called from the API

my_dict = fetch_latest_price_releases(access_token, tickers[2])

#%%
# ### N.B.
# 
# Here we extract the prices and not the entire dataset, and this is saved as a dictionary called 'spark_prices'.



data_points = my_dict["data"][0]["dataPoints"]

for data_point in data_points:
    period_start_at = data_point["deliveryPeriod"]["startAt"]

    spark_prices = dict()
    for unit, prices in data_point["derivedPrices"].items():
        spark_prices[unit] = prices["spark"]

    print(spark_prices)



#%%
# ## 3. Historical Prices
# 
# Here we perform a similar task, but with historical prices instead. This is done using the URL:
# 
# /v1.0/contracts/{contract_ticker_symbol}/price-releases/{limit}{offset}
# 
# First we define the function that imports the data from the Spark API.
# 
# We then call that function, and define 2 parameters:
# - 'tickers': which ticker do you want to call.
#     - We define the variable 'my_ticker' after the function definition, and set this to 'tickers[2]' which corresponds to Spark25s
#     - Alter this variable to whatever price product you need.
# - 'limit': this allows you to control how many datapoints you want to call. Here we use 'limit=10', which means we have called the last 10 datapoints (the Spark25 spot price for the last 10 business days).
#     - Alter this limit to however many datapoints you need.
# 
# 
# We save the output as a local variable called 'my_dict_hist'



def fetch_historical_price_releases(access_token, ticker, limit=4, offset=None):
    """
    For a selected contract, this endpoint returns all the Price Releases you can
    access according to your current subscription, ordered by release date descending.

    **Note**: Unlimited access to historical data and full forward curves is only
    available to those with Premium access. Get in touch to find out more.

    **Params**

    limit: optional integer value to set an upper limit on the number of price
           releases returned by the endpoint. Default here is 4.

    offset: optional integer value to set from where to start returning data.
            Default is 0.

    # Procedure:

    Do GET queries to /v1.0/contracts/{contract_ticker_symbol}/price-releases/
    with a Bearer token authorization HTTP header.
    """
    print(">>>> Get price releases for {}".format(ticker))

    query_params = "?limit={}".format(limit)
    if offset is not None:
        query_params += "&offset={}".format(offset)

    content = do_api_get_query(
        uri="/v1.0/contracts/{}/price-releases/{}".format(ticker, query_params),
        access_token=access_token,
    )

    my_dict = content['data']
    
    for release in content["data"]:
        release_date = release["releaseDate"]

        print("- release date =", release_date)

        data_points = release["data"][0]["dataPoints"]

        for data_point in data_points:
            period_start_at = data_point["deliveryPeriod"]["startAt"]

            spark_prices = dict()
            for unit, prices in data_point["derivedPrices"].items():
                spark_prices[unit] = prices["spark"]

            print(
                f"Spark Price={spark_prices} for period starting on {period_start_at}"
            )
    
    
    return my_dict



### Define which price product you want to retrieve
my_ticker = tickers[2]




if __name__ == "__main__":

    my_dict_hist = fetch_historical_price_releases(access_token, my_ticker, limit=10)



#%%

# ### Formatting into a Pandas DataFrame
# 
# The outputted data has several nested lists and dictionaries. If we are aware of what variables we want, we can externally store these values as lists and create a Pandas DataFrame.
# 
# Here we create empty lists for variables:
# - Release Dates
# - Start of Period
# - Ticker
# - Price in dollars/day
# - Price in dollars/MMBtu
# - The spread of the data used to calculate the Spot Price
#     - Min
#     - Max
# 
# 
# We then iterate through 'my_dict_hist' and append each value to the lists.
# 
# These lists can then be used to create our final Pandas DataFrame. 



release_dates = []
period_start = []
ticker = []
usd_day = []
usd_mmbtu = []

day_min = []
day_max = []

for release in my_dict_hist:
        release_date = release["releaseDate"]
        ticker.append(release['contractId'])

        print("- release date =", release_date)
        release_dates.append(release_date)

        data_points = release["data"][0]["dataPoints"]

        for data_point in data_points:
            period_start_at = data_point["deliveryPeriod"]["startAt"]
            period_start.append(period_start_at)

            usd_day.append(data_point['derivedPrices']['usdPerDay']['spark'])
            day_min.append(data_point['derivedPrices']['usdPerDay']['sparkMin'])
            day_max.append(data_point['derivedPrices']['usdPerDay']['sparkMax'])
            
            usd_mmbtu.append(data_point['derivedPrices']['usdPerMMBtu']['spark'])
            
            spark_prices = dict()
            for unit, prices in data_point["derivedPrices"].items():
                spark_prices[unit] = prices["spark"]

            

            print(
                f"Spark Price={spark_prices} for period starting on {period_start_at}"
            )



#%%
# Checking all the lists are the same size, and are equal to the 'limit' we set
# In this case, we set 'limit=10', so length of lists should be 10

print([len(release_dates), len(period_start), len(ticker), len(usd_day), len(usd_mmbtu)])


#%%

## Storing values in a Pandas DataFrame


historical_df = pd.DataFrame({
    'ticker': ticker,
    'Period Start': period_start,
    'USDperday': usd_day,
    'USDperdayMax': day_max,
    'USDperdayMin': day_min,
    'USDperMMBtu': usd_mmbtu,
    'Release Date': release_dates})
    
    
print(historical_df)



#%%
# ## 4. Analysing Data
# 
# Now that we have a DataFrame, statistical and analytical functions can conveniently be performed on the data to produce your own analytical products.
# 
# An example is provided below.

# ### 4.1 Prepping Data
# 
# All data called in from the API is in the string format. To perform statiscal analysis, or to plot the data, we need to change the relevant column data types from strings to numerics.



historical_df['USDperday'] = pd.to_numeric(historical_df['USDperday'])
historical_df['USDperdayMax'] = pd.to_numeric(historical_df['USDperdayMax'])
historical_df['USDperdayMin'] = pd.to_numeric(historical_df['USDperdayMin'])

historical_df['USDperMMBtu'] = pd.to_numeric(historical_df['USDperMMBtu'])


# We can then use the Pandas '.describe()' function to call basic statistics on a column.

#%%


historical_df['USDperday'].describe()

#%%
# ## 4.2 Plotting
# 
# ### 4.2.1 Matplotlib
# 
# Next, we can plot a simple timeseries of the data provided.
# 
# Below we use the standard matplotlib library to plot a simple timeseries.



import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style()
sns.set_theme(style="darkgrid")

fig, ax = plt.subplots(figsize=(15,9))
ax.plot_date(historical_df['Release Date'], historical_df['USDperday'])
ax.plot(historical_df['Release Date'], historical_df['USDperday'], color='blue')

ax.set_title(my_ticker)

ax.set_xlim(historical_df['Release Date'].iloc[-1], historical_df['Release Date'].iloc[0])
plt.xlabel('Price Release Date')
plt.ylabel('USD per day')



#%%
# #### Plot 2
# 
# The same plot, but with the min-max interval shaded in



fig, ax = plt.subplots(figsize=(15,9))
ax.plot_date(historical_df['Release Date'], historical_df['USDperday'])
ax.plot(historical_df['Release Date'], historical_df['USDperday'], color='blue')

ax.plot(historical_df['Release Date'], historical_df['USDperdayMin'], color='tab:blue', alpha=0.1)
ax.plot(historical_df['Release Date'], historical_df['USDperdayMax'], color='tab:blue', alpha=0.1)
ax.fill_between(historical_df['Release Date'], historical_df['USDperdayMin'], historical_df['USDperdayMax'], alpha=0.2)

ax.set_ylim(100000, 250000)
ax.set_xlim(historical_df['Release Date'].iloc[-1], historical_df['Release Date'].iloc[0])

ax.set_title(my_ticker)
plt.xlabel('Price Release Date')
plt.ylabel('USD per day')


#%%

# # Analysis Example 2: Yearly Comparisons
# 
# Here we use the full extent of the historical data for 'Spark30s'. We then organise this into a DataFrame and add additional descriptive columns so that we can compare Spark30s prices over the years.
# 
# First, we import the data with a new ticker ('my_ticker2') and a new limit of 1000.

#%%


my_ticker2 = tickers[6]

print(my_ticker)

if __name__ == "__main__":

    my_dict_hist2 = fetch_historical_price_releases(access_token, my_ticker2, limit=1000)


#%%# ### Store data in lists and reorganise into a DataFrame




release_dates2 = []
period_start2 = []
ticker2 = []
usd_day2 = []
usd_mmbtu2 = []

day_min2 = []
day_max2 = []

for release in my_dict_hist2:
        release_date2 = release["releaseDate"]
        ticker2.append(release['contractId'])

        print("- release date =", release_date2)
        release_dates2.append(release_date2)

        data_points = release["data"][0]["dataPoints"]

        for data_point in data_points:
            period_start_at = data_point["deliveryPeriod"]["startAt"]
            period_start2.append(period_start_at)

            usd_day2.append(data_point['derivedPrices']['usdPerDay']['spark'])
            day_min2.append(data_point['derivedPrices']['usdPerDay']['sparkMin'])
            day_max2.append(data_point['derivedPrices']['usdPerDay']['sparkMax'])
            
            usd_mmbtu2.append(data_point['derivedPrices']['usdPerMMBtu']['spark'])
            
            spark_prices = dict()
            for unit, prices in data_point["derivedPrices"].items():
                spark_prices[unit] = prices["spark"]

            

            print(
                f"Spark Price={spark_prices} for period starting on {period_start_at}"
            )





#%%## Storing values in a Pandas DataFrame


import pandas as pd

historical_df2 = pd.DataFrame({
    'ticker': ticker2,
    'Period Start': period_start2,
    'USDperday': usd_day2,
    'USDperdayMax': day_max2,
    'USDperdayMin': day_min2,
    'USDperMMBtu': usd_mmbtu2,
    'Release Date': release_dates2})
    
    
historical_df2['USDperday'] = pd.to_numeric(historical_df2['USDperday'])
historical_df2['USDperdayMax'] = pd.to_numeric(historical_df2['USDperdayMax'])
historical_df2['USDperdayMin'] = pd.to_numeric(historical_df2['USDperdayMin'])

historical_df2['USDperMMBtu'] = pd.to_numeric(historical_df2['USDperMMBtu'])


#%% ### Plot data



fig, ax = plt.subplots(figsize=(15,9))
ax.plot_date(historical_df2['Release Date'], historical_df2['USDperday'])
ax.plot(historical_df2['Release Date'], historical_df2['USDperday'], color='blue')

ax.plot(historical_df2['Release Date'], historical_df2['USDperdayMin'], color='tab:blue', alpha=0.1)
ax.plot(historical_df2['Release Date'], historical_df2['USDperdayMax'], color='tab:blue', alpha=0.1)
ax.fill_between(historical_df2['Release Date'], historical_df2['USDperdayMin'], historical_df2['USDperdayMax'], alpha=0.2)

#ax.set_ylim(100000, 250000)
ax.set_xlim(historical_df2['Release Date'].iloc[-1], historical_df2['Release Date'].iloc[0])

ax.set_title(my_ticker2)
plt.xlabel('Price Release Date')
plt.ylabel('USD per day')


#%%# ## Sorting Data
# 
# We then create new columns to categorise the data we have. 
# - By creating the 'Years' column, we can filter the data by year.
# - By creating the 'Day of Year' column, we can plot the yearly data on the same x-axis range.



from datetime import datetime

years = []
months = []
days = []
monthday = []
dayofyear = []
for r in release_dates2:
    rsplit = r.split('-')
    years.append(rsplit[0])
    months.append(rsplit[1])
    days.append(rsplit[2])
    
    dayofyear.append(datetime.strptime(r, '%Y-%m-%d').timetuple().tm_yday)
    
    md = datetime.strptime(rsplit[1]+'-'+rsplit[2], '%m-%d')
    monthday.append(md)

historical_df2['Year'] = years
historical_df2['Month'] = months
historical_df2['Day'] = days
historical_df2['Yearly Date'] = monthday
historical_df2['Day of Year'] = dayofyear

historical_df2.head()



#%%# Filter data by year

#print(historical_df2[historical_df2['Year']=='2019']['USDperday'])


#%%# ## Plotting Data to create our Yearly Comparison chart


fig, ax = plt.subplots(figsize=(15,9))
year_list = ['2019','2020','2021','2022','2023']
colour_list = ['turquoise', 'darkorange', 'purple', 'blue']

for y in year_list:
    hdf = historical_df2[historical_df2['Year']==y]
    if y != '2023':       
        ax.plot(hdf['Day of Year'], hdf['USDperday'], color= colour_list[year_list.index(y)], alpha=0.3)
    else:
        ax.plot(hdf['Day of Year'], hdf['USDperday'], color = 'green')

        ax.plot(hdf['Day of Year'], hdf['USDperdayMin'], color = 'mediumspringgreen', alpha=0.1)
        ax.plot(hdf['Day of Year'], hdf['USDperdayMax'], color = 'mediumspringgreen', alpha=0.1)
        ax.fill_between(hdf['Day of Year'], hdf['USDperdayMin'], hdf['USDperdayMax'], color = 'mediumspringgreen', alpha=0.2)


ax.set_title('Yearly Comparison of Spark30S Prices')
plt.xlabel('Price Release Date')
plt.ylabel('USD per day')
plt.legend(year_list)

# Setting custom x-axis ticks location and labels.
xlabels = ['January', 'March', 'May', 'July', 'September', 'November', 'Year End']

# xpos gives the first day of every other month in terms of 'day of year'
#xpos_allmonths = [1,32,60,91,121,152,182,213,244,274,305,335]
xpos = [1,60,121,182,244,305,365]

plt.xticks(xpos, xlabels)

current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels(['$ {:,.0f}'.format(x) for x in current_values])


# In[ ]:







