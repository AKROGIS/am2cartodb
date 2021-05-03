# -*- coding: utf-8 -*-
"""
Simple tests of the Carto (SQL based mapping database) API.

You must have a Carto (https://carto.com) (formerly Cartodb) account and
apikey - these are set in the `carto_secrets.py` file.  Note that the apikey is
not required for select statements on a public table.  This code will work with
an hosted server on carto.com, or an on premises installation. Modify the
base_url value in the Config object as needed.

The Carto module provides a connection that acts like a database server i.e. it
takes an SQL statement and returns a list of rows by wrapping a ReST request and
response.

Example SQL request in the browser:
  Hosted:
  https://{user}.carto.com/api/v2/sql?api_key={apikey}&q={sql}
  On Premises
  https://{domain}/user/{user}/api/v2/sql?api_key={apikey}&q={sql}

Carto references (2021-02-01):
https://github.com/CartoDB/carto-python
https://carto.com/developers/python-sdk/
https://carto-python.readthedocs.io/en/latest/carto.html#module-carto.sql

If the on-premises carto server is using a self signed certificate, or if
the Python `certifi` module does not have all of the necessary certificates
to verify the server (happed with the nps server in 2021), then you will
need to use the carto.auth.NonVerifiedAPIKeyAuthClient authorization,
and/or the request with `verify=False` (see the commented lines in the code).
I was able to add the necessary certificates by checking with the browser.
In Chrome visit the server and click on the lock icon, and find information
on the certificate. Click the download certificate chain as PEM, and add it to
the end of the file `cacert.pem` in the `certifi` module in the Python
`site-packages`.

Third party requirements:
* carto - https://pypi.python.org/pypi/carto
  (for large and/or authenticated requests)
* requests - https://pypi.python.org/pypi/requests
  (for simple non-authenticated GET requests)
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import sys

from carto.auth import APIKeyAuthClient

# To disable SSL certificate verification (unsafe)
# from carto.auth import NonVerifiedAPIKeyAuthClient
from carto.sql import SQLClient, CartoException
import requests

import carto_secrets

# Python 2/3 compatible xrange() cabability
# pylint: disable=undefined-variable,redefined-builtin
if sys.version_info[0] < 3:
    range = xrange


class Config(object):
    """Namespace for configuration parameters. Edit as necessary."""

    # pylint: disable=useless-object-inheritance,too-few-public-methods

    # On premises  Carto server
    base_url = "https://carto.nps.gov/user/{user}/".format(user=carto_secrets.user)
    # Hosted Carto server
    # May vary if you have an organizational account or not
    # The first line still works for reading, however as of 2021, it appears
    # the hosted account no longer allows edits or any account modifications.
    # so write testing was not possible.
    # base_url = "https://{user}.carto.com/"
    # base_url = "https://nationalparkservice.carto.com/user/{user}/"

    # For public GET requests
    sql_url = base_url + "api/v2/sql/"

    # A look up table of testing/configuration queries.
    # The first X queries create and modify new test tables. They have the
    # same name as the production tables with a numerical suffix. Do not
    # remove the suffix unless you want to screw up the production tables.
    # The remainder of the queries are read-only queries of the production
    # tables.
    queries = {
        # Create the table of animal location points
        1: """
            CREATE TABLE Animal_Locations5 (
                ProjectId text NOT NULL,
                AnimalId text NOT NULL,
                FixDate timestamp NOT NULL,
                FixId int NOT NULL
            )
        """,
        # Create the table of animal movement vectors (two point lines)
        2: """
            CREATE TABLE Animal_Movements5 (
                ProjectId text NOT NULL,
                AnimalId text NOT NULL,
                StartDate timestamp NOT NULL,
                EndDate timestamp NOT NULL,
                Duration real NOT NULL,
                Distance real NOT NULL,
                Speed real NOT NULL
            )
        """,
        # Add spatial columns (cartod-ify) to the locations table
        3: "SELECT cdb_cartodbfytable('{0}', 'Animal_Locations5')".format(
            carto_secrets.user
        ),
        # Add spatial columns (cartod-ify) to the locations table
        4: "SELECT cdb_cartodbfytable('{0}', 'Animal_Movements5')".format(
            carto_secrets.user
        ),
        # Create a new Animal Location record
        5: """
            INSERT INTO Animal_Locations5
            (ProjectId, AnimalId, FixDate, the_geom, FixId)
            VALUES (
                'KATM_BrownBear',
                '065',
                '2015-05-27 23:30:45.0000000',
                ST_SetSRID(ST_Point(-153.380942, 58.817325), 4326),
                2612928
            )
        """,
        # Create a new Animal Movement record
        6: """
            INSERT INTO Animal_Movements5
            (ProjectId, AnimalId, StartDate, EndDate, Duration, Distance, the_geom)
            VALUES (
                'KATM_BrownBear',
                '065',
                '2015-05-27 23:30:45.0000000',
                '2015-05-28 23:30:45.0000000',
                24.0,
                12.345,
                ST_SetSRID(
                    ST_GeomFromText('LINESTRING(-153.381 58.817, -153.480 58.765)'),
                    4326
                )
            )
        """,
        # Update the first record of the Animal Locations table.
        7: """
            UPDATE Animal_Locations5
            SET the_geom = ST_SetSRID(ST_Point(-153.900132, 58.577771), 4326)
            WHERE cartodb_id = 1
        """,
        # Update the first record of the Animal Movement table.
        8: """
            UPDATE Animal_Movements5
            SET Speed = 0.456
            WHERE cartodb_id = 1
        """,
        # Delete the Animal Locations table
        9: "DROP TABLE Animal_Locations5",
        # Delete the Animal Movements table
        10: "DROP TABLE Animal_Movements5",
        # Read only public access queries
        # Get a count of the records in the Animal Locations table
        11: "SELECT count(*) FROM Animal_Locations",
        # Get the top 10 Location records in database creation order.
        12: "SELECT * FROM Animal_Locations ORDER BY cartodb_id LIMIT 10",
        # Get a portion of the first 10 Location records in timestamp order.
        13: """
            SELECT FixDate, FixId, ST_AsGeoJSON(the_geom)
            FROM Animal_Locations ORDER BY FixDate LIMIT 10
        """,
        # Get the date range of the Location records
        14: """
            SELECT min(FixDate) AS First, max(FixDate) AS Last
            FROM Animal_Locations
        """,
        # Get a count of the records in the Animal Movements table
        15: "SELECT count(*) FROM Animal_Movements",
        # Get the top 10 Movement records in database creation order.
        16: "SELECT * FROM Animal_Movements ORDER BY cartodb_id LIMIT 10",
        # Get a portion of the first 10 Movement records in timestamp order.
        17: """
            SELECT StartDate, ST_AsGeoJSON(the_geom)
            FROM Animal_Movements ORDER BY StartDate LIMIT 10
        """,
        # Get the date range of the Movement records
        18: """
            SELECT min(StartDate) AS First, max(StartDate) AS Last
            FROM Animal_Movements
        """,
    }


def get_auth_carto_sql_connection():
    """Return a authorized SQL connection to the carto database, using the secrets."""

    # To disable SSL certificate verification (unsafe)
    # auth_client = NonVerifiedAPIKeyAuthClient(
    auth_client = APIKeyAuthClient(
        api_key=carto_secrets.apikey,
        base_url=Config.base_url,
    )
    return SQLClient(auth_client)


def auth_query(query):
    """Run a test query and print the results (or error)."""

    connection = get_auth_carto_sql_connection()
    print(query)
    try:
        data = connection.send(query)
        print(data)
    except CartoException as ex:
        print("some error ocurred {0}".format(ex))


def auth_queries():
    """Run all authenticated test queries and print the results (or error)."""

    for i in range(1, 11):
        print("\nTest #{0}: ".format(i), end="")
        auth_query(Config.queries[i])


def public_query(query):
    """Run a public (non-authenticated test query and print the results (or error)."""

    url = "{0}?q={1}".format(Config.sql_url, query)
    print(url)
    try:
        # To disable SSL certificate verification (unsafe)
        # result = requests.get(url, verify=False)
        result = requests.get(url)
        print(result.json())
    except requests.exceptions.RequestException as ex:
        print("Some error ocurred {0}".format(ex))


def public_queries():
    """Run all public (non-authenticated) test queries and print the results (or error)."""

    for i in range(11, 19):
        print("\nTest #{0}: ".format(i), end="")
        public_query(Config.queries[i])


# Run a single adhoc public query
# public_query("select count(*) from animal_locations")

# Run a single adhoc authenticated query
# auth_query("select count(*) from animal_locations")

# Run a single predefined public query
# public_query(Config.queries[15])

# Run a single authenticated query
# auth_query(Config.queries[1])

# Run all public query
public_queries()

# Run all authenticated query
# auth_queries()
