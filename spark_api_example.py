#!/usr/bin/env python3

"""
Spark API Python example.

This script retrieves an access token then fetches available Spark contracts.

Usage:

$ python spark_api_example.py

Requires Python 3 (version >=3.1)
"""

import json
import os
from base64 import b64encode

try:
    from urllib import request, parse
except ImportError:
    raise RuntimeError("Python 3 required")


# Your credentials. You can edit this file or set environment variables.
# Please send an email to info@sparkcommodities.com to request credentials.
CLIENT_ID = os.getenv("SPARK_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPARK_CLIENT_SECRET")
if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError("SPARK_CLIENT_ID and SPARK_CLIENT_SECRET env vars required")


API_BASE_URL = "https://api.sparkcommodities.com"


def get_access_token():
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

    url = "{}/oauth/token/".format(API_BASE_URL)

    payload = "{}:{}".format(CLIENT_ID, CLIENT_SECRET).encode()
    headers = {
        "Authorization": b64encode(payload).decode(),
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    body = {"grantType": "clientCredentials", "scopes": "read:lng-freight-prices"}
    data = json.dumps(body).encode("utf-8")

    # POST request (data is not None)
    req = request.Request(url, data=data, headers=headers)
    response = request.urlopen(req)
    resp_content = response.read()

    # The server must return a HTTP 201. Raise an error if this is not the case
    assert response.status == 201, response.status

    # The server returned a JSON response
    content = json.loads(resp_content)

    print(
        "Successfully fetched an access token {}****, valid {} seconds.".format(
            content["accessToken"][:10], content["expiresIn"]
        )
    )

    return content["accessToken"]


def list_contracts(access_token):
    """
    Fetch available contracts.

    # Procedure:

    Do a GET query with a Bearer token authorization HTTP header.
    """

    url = "{}/v1.0/contracts/".format(API_BASE_URL)

    headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Accept": "application/json",
    }

    req = request.Request(url, headers=headers)
    response = request.urlopen(req)
    content = response.read()

    # The server must return a HTTP 200. Raise an error if this is not the case
    assert response.status == 200, content

    # The server returned a JSON response
    data = json.loads(content)

    print("Contracts:")
    for contract in data["data"]:
        print(contract["fullName"])


def main():
    access_token = get_access_token()
    list_contracts(access_token)


if __name__ == "__main__":
    main()
