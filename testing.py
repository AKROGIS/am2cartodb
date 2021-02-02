# -*- coding: utf-8 -*-
"""
Simple tests of the Carto (SQL based mapping database) API.

You must have a Carto (https://carto.com) (formerly Cartodb) account and
apikey - these are set in the secrets.py file.  Note that the apikey is not
required for select statements on a public table.

The Carto module provides a connection that acts like a database server e.i. it
takes an SQL statement and returns a list of rows by wraps a ReST request and
response.

Example SQL request in the browser:
  domain = {domain} aka username
  apikey = {apikey}
  sql = {sql}
  https://{domain}.carto.com/api/v2/sql?api_key={apikey}&q={sql}

Third party requirements:
* cartodb - https://pypi.python.org/pypi/cartodb
  (Note: This module is written for Python 2.5 and does not work with 3.x)
* requests - https://pypi.python.org/pypi/requests
  (for simple non-authenticated GET requests)
"""

from __future__ import absolute_import, division, print_function, unicode_literals

# from cartodb import CartoDBAPIKey, CartoDBException
import requests

import carto_secrets


class Config(object):
    """Namespace for configuration parameters. Edit as necessary."""

    # pylint: disable=useless-object-inheritance,too-few-public-methods

    base_url = "http://{user}.carto.com/api/v2/sql/".format(
        user=carto_secrets.domain
    )

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
            carto_secrets.domain
        ),
        # Add spatial columns (cartod-ify) to the locations table
        4: "SELECT cdb_cartodbfytable('{0}', 'Animal_Movements5')".format(
            carto_secrets.domain
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
                    ST_GeomFromText('LINESTRING(-153.381, 58.817, -153.480, 58.765)'),
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


def get_carto_connection():
    """Return a connection to the carto database, using the secrets."""

    return CartoDBAPIKey(carto_secrets.apikey, carto_secrets.domain)


def test(query):
    """Run a test query and print the results (or error)."""

    connection = get_carto_connection()
    try:
        result = connection.sql(query)
        print(result)
    except CartoDBException as ex:
        print("Some error ocurred {0}".format(ex))


def public_query(query):
    """Run a public (non-authenticated test query and print the results (or error)."""

    url = "{0}?q={1}".format(Config.base_url, query)
    print(url)
    try:
        result = requests.get(url)
        print(result.json())
    except requests.exceptions.RequestException as ex:
        print("Some error ocurred {0}".format(ex))

def public_tests():
    """Run all public (non-authenticated test queries and print the results (or error)."""

    for i in range(11, 19):
        print("\nTest #{0}: ".format(i), end="")
        public_query(Config.queries[i])


# test(Config.queries[15])
public_tests()
