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
* carto - https://pypi.python.org/pypi/carto  (formerly cartodb)
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from cartodb import CartoDBAPIKey, CartoDBException

import secrets


# example sql in browser:
# https://{domain}.cartodb.com/api/v2/sql?api_key={apikey}&q=update animal_movements set duration_t=round(cast(duration as numeric),1)

def test(c):
    #sql = "CREATE TABLE Animal_Locations (ProjectId text NOT NULL,AnimalId text NOT NULL,FixDate timestamp NOT NULL,FixId int NOT NULL)"
    #sql = "select cdb_cartodbfytable('"+secrets.domain+"','Animal_Locations')"
    #sql = "CREATE TABLE Animal_Movements(ProjectId text NOT NULL,AnimalId text NOT NULL,StartDate timestamp NOT NULL,EndDate timestamp NOT NULL,Duration real NOT NULL,Distance real NOT NULL,Speed real NOT NULL)"
    #sql = "select cdb_cartodbfytable('"+secrets.domain+"','Animal_Movements')"
    #sql = "insert into animal_locations (ProjectId,AnimalId,FixDate,the_geom,FixId) VALUES ('KATM_BrownBear','065','2015-05-27 23:30:45.0000000',ST_SetSRID(ST_Point( -153.380942,   58.817325),4326),2612928)"
    #sql = 'select * from animal_locations'
    #sql = 'select fixid,ST_AsGeoJSON(the_geom) from animal_locations'
    sql = 'select * from animal_movements'
    #sql = "update animal_locations set the_geom=ST_SetSRID(ST_Point(-153.900132,58.577771),4326) where cartodb_id = 1"
    try:
       print(c.sql(sql))
    except CartoDBException as e:
       print ("some error ocurred", e)

carto = CartoDBAPIKey(secrets.apikey, secrets.domain)
test(carto)
