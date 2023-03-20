# Spark API Code Sample In Python

Only Python >= 3.7 is required. 

## Usage

**An OAuth2 client is required.**

Please refer to https://app.sparkcommodities.com/freight/data-integrations/api if you 
don't have any.

### Running the script by providing the `client_credentials.csv` file path 

```shell
$ python spark_api_example.py <client_credentials_csv_file_path>
```

### Running the script by providing environment variables:

```shell
$ export SPARK_CLIENT_ID=XXXX
$ export CLIENT_SECRET=YYYY
$ python spark_api_example.py
```

### Execution Sample:

An execution example:

```shell
$ python spark_api_example.py ~/Downloads/client_credentials.csv
```

Output:

```
>>>> Running Spark API Python sample...
>>>> Found credentials!
>>>> Client_id=166f3ee3-cfdc-46cc-ad43-3c1894697bb4, client_secret=2a14d****
>>>> Successfully fetched an access token eyJ0e****, valid 1799 seconds.
>>>> Contracts:
Spark25S Pacific 160 TFDE
Spark25F Pacific 160 TFDE
Spark25Fo Pacific 160 TFDE
...
>>>> Get latest price release for spark25s
release date = 2020-12-14
Spark Price is USD 186000 /day for period starting on 2020-12-29
>>>> Get latest price release for spark25f
release date = 2020-12-14
Spark Price is USD 187500 /day for period starting on 2020-01-01
>>>> Get latest price release for spark25fo
release date = 2020-12-14
Spark Price is USD 192250 /day for period starting on 2021-01-01
Spark Price is USD 128000 /day for period starting on 2021-02-01
...
>>>> Well done, you successfully ran your first queries!
```
