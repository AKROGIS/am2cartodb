__author__ = 'RESarwas'
import sys

# dependency pyodbc
# C:\Python27\ArcGIS10.3\Scripts\pip.exe install pyodbc
# dependency cartodb
# C:\Python27\ArcGIS10.3\Scripts\pip.exe install cartodb

import secrets

try:
    from cartodb import CartoDBAPIKey, CartoDBException
except ImportError:
    CartoDBAPIKey, CartoDBException = None, None
    print 'cartodb module not found, make sure it is installed with'
    print 'C:\Python27\ArcGIS10.3\Scripts\pip.exe install cartodb'
    sys.exit()

try:
    import pyodbc
except ImportError:
    pyodbc = None
    print 'pyodbc module not found, make sure it is installed with'
    print 'C:\Python27\ArcGIS10.3\Scripts\pip.exe install pyodbc'
    sys.exit()


def get_connection_or_die():
    conn_string = ("DRIVER={{SQL Server Native Client 10.0}};"
                   "SERVER={0};DATABASE={1};Trusted_Connection=Yes;")
    conn_string = conn_string.format('inpakrovmais', 'animal_movement')
    try:
        connection = pyodbc.connect(conn_string)
    except pyodbc.Error as e:
        print("Rats!!  Unable to connect to the database.")
        print("Make sure your AD account has the proper DB permissions.")
        print("Contact Regan (regan_sarwas@nps.gov) for assistance.")
        print("  Connection: " + conn_string)
        print("  Error: " + e[1])
        sys.exit()
    return connection


def make_cartodb_tracking_table(connection):
    sql = "create table Locations_In_CartoDB (fixid int NOT NULL PRIMARY KEY)"
    wcursor = connection.cursor()
    wcursor.execute(sql)
    try:
        wcursor.commit()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        print ("Unable to add create the 'Locations_In_CartoDB' table.")


def add_ids_to_carto_tracking_table(connection, fids):
    wcursor = connection.cursor()
    for fid in fids:
        sql = "INSERT Locations_In_CartoDB (fixid)values({0})"
        sql = sql.format(fid)
        # print(sql)
        wcursor.execute(sql)
    try:
        wcursor.commit()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        print ("Unable to add these ids to the 'Locations_In_CartoDB' table.")
        print (fids)


def get_locations_for_carto(connection):
    sql = ("select projectid,animalid,l.fixid,fixdate,"
           "location.Lat,Location.Long from locations as l "
           "left join Locations_In_CartoDB as c on l.fixid = c.fixid "
           "where ProjectID = 'KATM_BrownBear' and c.fixid IS NULL "
           "and [status] IS NULL")
    # TODO ensure we are not passing any locations outside the park < 30 days
    rcursor = connection.cursor()
    try:
        rows = rcursor.execute(sql).fetchall()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        rows = None
    return rows


def fixlocationrow(row):
    s = "('{0}','{1}',{2},'{3}',ST_SetSRID(ST_Point({5},{4}),4326))"
    return s.format(*row)


def insert(am, carto, rows):
    if not rows:
        print('No new locations to send to CartoDB.')
        return
    sql = ("insert into animal_locations "
           "(projectid,animalid,fixid,fixdate,the_geom) values ")
    ids, values = zip(*[(row[2], fixlocationrow(row)) for row in rows])
    values = ','.join(values)
    try:
        carto.sql(sql + values)
        try:
            add_ids_to_carto_tracking_table(am, ids)
            print('Wrote ' + str(len(ids)) + ' locations to CartoDB.')
        except pyodbc.Error as de:
            print ("Database error ocurred", de)
    except CartoDBException as ce:
        print ("CartoDB error ocurred", ce)


carto_conn = CartoDBAPIKey(secrets.apikey, secrets.domain)
am_conn = get_connection_or_die()
# make_cartodb_tracking_table(am_conn)
locations = get_locations_for_carto(am_conn)
insert(am_conn, carto_conn, locations)

# TODO add movement vectors
# TODO if status changes, then I should update locations (and vectors)
