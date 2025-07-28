#!/usr/bin/env python3

"""
Spark API Python example.

This script retrieves an access token then fetches available Spark contracts.
It requires Python 3 (version >=3.1)

Usage:

- By providing the client_credentials file path

$ python spark_price_releases.py <client_credentials_csv_file_path>

- By providing 2 environment variables:

$ export SPARK_CLIENT_ID=XXXX
$ export SPARK_CLIENT_SECRET=YYYY
$ python spark_price_releases.py
"""

import json
import os
import sys
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

    Do a POST query with `grantType` in the body. A basic authorization
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

    print(">>>> All the contracts you can fetch")
    tickers = []
    for contract in content["data"]:
        print(contract["fullName"])
        tickers.append(contract["id"])

    return tickers


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


def fetch_routes(access_token):
    print(">>>> List the first 10 routes")

    # list all available routes and release date:
    routes = do_api_get_query(
        uri="/v1.0/routes",
        access_token=access_token,
    )

    for route in routes["data"]["routes"][:10]:
        print(
            f"uuid={route['uuid']}, "
            f"{route['loadPort']['name']} to {route['dischargePort']['name']} "
            f"via={route['via']})"
        )

    release_dates = routes["data"]["sparkReleaseDates"]

    print(
        ">>>> I'm interested in the first route, I want to print the last released cost"
    )
    route_uuid = routes["data"]["routes"][0]["uuid"]
    for release_date in release_dates[:1]:
        ship_costs = do_api_get_query(
            uri=f"/v1.0/routes/{route_uuid}?release-date={release_date}",
            access_token=access_token,
        )
        print(f"ship costs on {release_date}")
        pprint(ship_costs["data"])


def fetch_intraday(access_token):
    print(">>>> List last Intraday items (JKM-TTF / usb-per-mmbtu)")

    # list all available routes and release date:
    content = do_api_get_query(
        uri=f"/beta/intraday/live?contract=jkm-ttf&unit=usd-per-mmbtu",
        access_token=access_token,
    )

    for data_point in content["data"]:
        print(
            f"asOf={data_point['asOf']}, "
            f"period={data_point['periodName']}, "
            f"revision={data_point['revision']}): "
            f"{data_point['value']})"
        )

def main(file_path=None):
    print(">>>> Running Spark API Python sample...")

    client_id, client_secret = retrieve_credentials(file_path)

    # Authenticate:
    access_token = get_access_token(client_id, client_secret)

    # Fetch all contracts:
    tickers = list_contracts(access_token)

    # For only 1 contract:
    fetch_latest_price_releases(access_token, tickers[0])

    # For only 1 contract:
    fetch_historical_price_releases(access_token, tickers[0], limit=4)

    # Routes (only a few ones)
    fetch_routes(access_token)

    # Intraday live data
    fetch_intraday(access_token)

    print(">>>> Well done, you successfully ran your first queries!")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        main(file_path=sys.argv[1])
    else:
        main()
