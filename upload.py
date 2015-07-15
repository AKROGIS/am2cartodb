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


def make_location_table_in_cartodb(carto):
    sql = ("CREATE TABLE Animal_Locations (ProjectId text NOT NULL, AnimalId text NOT NULL, "
           "FixDate timestamp NOT NULL, FixId int NOT NULL)")
    execute_sql_in_cartodb(carto, sql)


def make_movement_table_in_cartodb(carto):
    sql = ("CREATE TABLE Animal_Movements (ProjectId text NOT NULL, AnimalId text NOT NULL, "
           "StartDate timestamp NOT NULL, EndDate timestamp NOT NULL, Duration real NOT NULL, "
           "Distance real NOT NULL, Speed real NOT NULL)")
    execute_sql_in_cartodb(carto, sql)


def execute_sql_in_cartodb(carto, sql):
    try:
        carto.sql(sql)
    except CartoDBException as ce:
        print ("CartoDB error ocurred", ce)


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


def get_vectors_for_carto(connection):
    sql = ("select m.Projectid, m.AnimalId, m.StartDate, m.EndDate, m.Duration, m.Distance, m.Speed, "
           "m.Shape.ToString() from movements as m left join locations as l "
           "on m.ProjectId = l.ProjectId and m.AnimalId = l.AnimalId and m.EndDate = l.FixDate "
           "left join Locations_In_CartoDB as c on l.fixid = c.fixid "
           "where M.ProjectId = 'KATM_BrownBear' and c.fixid IS NOT NULL and Distance > 0")
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


def fixmovementrow(row):
    s = "('{0}','{1}','{2}','{3}',{4},{5},{6},ST_GeometryFromText('{7}',4326))"
    return s.format(*row)


def insert(am, carto, lrows, vrows):
    if not lrows and not vrows:
        print('No new data to send to CartoDB.')
        return
    try:
        if vrows:
            sql = ("insert into animal_movements "
                   "(projectid, animalid, startdate, enddate, duration, distance, speed, the_geom) values ")
            values = ','.join([fixmovementrow(row) for row in vrows])
            # print sql + values
            carto.sql(sql + values)
        if lrows:
            sql = ("insert into animal_locations "
                   "(projectid,animalid,fixid,fixdate,the_geom) values ")
            ids, values = zip(*[(row[2], fixlocationrow(row)) for row in lrows])
            values = ','.join(values)
            # print sql + values
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
vectors = get_vectors_for_carto(am_conn)
insert(am_conn, carto_conn, locations, vectors)


# TODO add movement vectors
# TODO if status changes, then I should update locations (and vectors)
