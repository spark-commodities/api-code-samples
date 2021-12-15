#!/usr/bin/env python3

"""
Spark API Python example.

This script retrieves an access token then fetches available Spark contracts.
It requires Python 3 (version >=3.1)

Usage:

- By providing the client_credentials file path

$ python spark_api_example.py <client_credentials_csv_file_path>

- By providing 2 environment variables:

$ export SPARK_CLIENT_ID=XXXX
$ export CLIENT_SECRET=YYYY
$ python spark_api_example.py
"""

import json
import os
import sys
from base64 import b64encode
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
            first_line = fp.readline()
            if "clientId,clientSecret" not in first_line:
                print("First line: {}".format(first_line))
                raise RuntimeError(
                    "The specified file {} doesn't look like to be a Spark API client "
                    "credentials file".format(file_path)
                )

            second_line = fp.readline()

        client_id, client_secret = second_line.split(",")

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
        "scopes": "read:lng-freight-prices",
    }

    content = do_api_post_query(uri="/oauth/token/", body=body, headers=headers)

    print(
        ">>>> Successfully fetched an access token {}****, valid {} seconds.".format(
            content["accessToken"][:5], content["expiresIn"]
        )
    )

    return content["accessToken"]


#
# Spark data fetching functions:
#


def list_contracts(access_token):
    """
    Fetch available contracts. Return contract ticker symbols

    # Procedure:

    Do a GET query to /v1.0/contracts/ with a Bearer token authorization HTTP header.
    """
    content = do_api_get_query(uri="/v1.0/contracts/", access_token=access_token)

    print(">>>> Contracts:")
    tickers = []
    for contract in content["data"]:
        print(contract["fullName"])
        tickers.append(contract["id"])

    return tickers


def get_latest_price_releases(access_token, ticker):
    """
    For each contract, fetch then display the latest price release

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
        spark_price = data_point["derivedPrices"]["usdPerDay"]["spark"]

        print(
            "Spark Price is USD",
            "{:>6}".format(spark_price),
            "/day for period starting on",
            period_start_at,
        )


def main(file_path=None):

    print(">>>> Running Spark API Python sample...")

    client_id, client_secret = retrieve_credentials(file_path)

    # Authenticate:
    access_token = get_access_token(client_id, client_secret)

    # Fetch data:
    tickers = list_contracts(access_token)
    for ticker in tickers:
        get_latest_price_releases(access_token, ticker)

    print(">>>> Done!")


if __name__ == "__main__":

    if len(sys.argv) >= 2:
        main(file_path=sys.argv[1])
    else:
        main()
