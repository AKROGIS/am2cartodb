# -*- coding: utf-8 -*-
"""
A python tool for publishing select data from the
[Animal Movement](https://github.com/AKROGIS/AnimalMovement)
database (an internal SQL Server) to a public [Carto](https://carto.com)
database.  The tool keeps track of what data has already been published, and
only pushes changes since the last run.  It is best run as a scheduled task.

Biologists with data in Animal Movements must elect to publish their data to
Carto.  Currently only the Katmai Bear data is being published. Locations are
filtered by project and location (if an animal goes outside the protection of
the park, its location is not published).

You must have a Carto (https://carto.com) (formerly Cartodb) account and
apikey - these are set in the `secrets.py` file.  See `testing_carto.py`
for a simple example, with explanaitons.

Third party requirements:
* carto - https://pypi.python.org/pypi/carto  (formerly cartodb)
* pyodbc - https://pypi.python.org/pypi/pyodbc - for SQL Server
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys

import secrets


# Python 2/3 compatible xrange cabability
# pylint: disable=undefined-variable,redefined-builtin
if sys.version_info[0] < 3:
    range = xrange


def module_missing(name):
    print('Module {0} not found, make sure it is installed.'.format(name))
    exec_dir = os.path.split(sys.executable)[0]
    pip = os.path.join(exec_dir, 'Scripts', 'pip.exe')
    if not os.path.exists(pip):
        print("First install pip. See instructions at: "
               "'https://pip.pypa.io/en/stable/installing/'.")
    print('Install with: {0} install {1}'.format(pip, name))
    sys.exit()


try:
    from cartodb import CartoDBAPIKey, CartoDBException
except ImportError:
    CartoDBAPIKey, CartoDBException = None, None
    module_missing('cartodb')

try:
    import pyodbc
except ImportError:
    pyodbc = None
    module_missing('pyodbc')


def get_connection_or_die():
    conn_string = ("DRIVER={{SQL Server Native Client 11.0}};"
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
    sql = "select cdb_cartodbfytable('"+secrets.domain+"','Animal_Locations')"
    execute_sql_in_cartodb(carto, sql)


def make_movement_table_in_cartodb(carto):
    sql = ("CREATE TABLE Animal_Movements (ProjectId text NOT NULL, AnimalId text NOT NULL, "
           "StartDate timestamp NOT NULL, EndDate timestamp NOT NULL, Duration real NOT NULL, "
           "Distance real NOT NULL, Speed real NOT NULL, "
           "Duration_t text NULL, Distance_t text NULL, Speed_t text NULL)")
    execute_sql_in_cartodb(carto, sql)
    sql = "select cdb_cartodbfytable('"+secrets.domain+"','Animal_Movements')"
    execute_sql_in_cartodb(carto, sql)


def execute_sql_in_cartodb(carto, sql):
    try:
        carto.sql(sql)
    except CartoDBException as ce:
        print ("CartoDB error ocurred", ce)


def chunks(l, n):
    """Yield successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]


def make_cartodb_tracking_tables(connection):
    sql = ("if not exists (select * from sys.tables where name='Locations_In_CartoDB')"
           "  create table Locations_In_CartoDB (fixid int NOT NULL PRIMARY KEY)")
    sql2 = ("if not exists (select * from sys.tables where name='Movements_In_CartoDB')"
            "  create table Movements_In_CartoDB ("
            "    ProjectId varchar(16) NOT NULL,"
            "    AnimalId varchar(16) NOT NULL,"
            "    StartDate datetime2(7) NOT NULL,"
            "    EndDate datetime2(7) NOT NULL"
            "    CONSTRAINT PK_Movements_In_CartoDB PRIMARY KEY CLUSTERED ("
            "      ProjectId ASC, AnimalId ASC, StartDate ASC, EndDate ASC))")
    wcursor = connection.cursor()
    wcursor.execute(sql)
    wcursor.execute(sql2)
    try:
        wcursor.commit()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        print ("Unable to add create the 'Locations_In_CartoDB' table.")


def add_locations_to_carto_tracking_table(connection, fids):
    # SQL Server is limited to 1000 rows in an insert
    wcursor = connection.cursor()
    sql = "INSERT Locations_In_CartoDB (fixid) values "
    for chunk in chunks(fids, 900):
        values = ','.join(["({0})".format(fid) for fid in chunk])
        # print(sql + values)
        wcursor.execute(sql + values)
    try:
        wcursor.commit()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        print ("Unable to add these ids to the 'Locations_In_CartoDB' table.")
        print (fids)


def add_movements_to_carto_tracking_table(connection, rows):
    # SQL Server is limited to 1000 rows in an insert
    wcursor = connection.cursor()
    sql = "insert into Movements_In_CartoDB (projectid, animalid, startdate, enddate) values "
    for chunk in chunks(rows, 900):
        values = ','.join(["('{0}','{1}','{2}','{3}')".format(*row) for row in chunk])
        # print(sql + values)
        wcursor.execute(sql + values)
    try:
        wcursor.commit()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        print ("Unable to add these rows to the 'Movements_In_CartoDB' table.")
        print (rows)


def remove_locations_from_carto_tracking_table(connection, fids):
    wcursor = connection.cursor()
    sql = "delete from Locations_In_CartoDB where fixid in "
    # Protection from really long lists, by executing multiple queries.
    for chunk in chunks(fids, 900):
        ids = '(' + ','.join(["{0}".format(fid) for fid in chunk]) + ')'
        wcursor.execute(sql + ids)
    try:
        wcursor.commit()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        print ("Unable to delete these ids from the 'Locations_In_CartoDB' table.")
        print (fids)


def remove_movements_from_carto_tracking_table(connection, rows):
    wcursor = connection.cursor()
    sql = ("delete from Movements_In_CartoDB where "
           "projectid = '{0}' and animalid = '{1}' and startdate = '{2}' and enddate = '{3}'")
    for row in rows:
        sql1 = sql.format(row[0], row[1], row[2], row[3])
        wcursor.execute(sql1)
    try:
        wcursor.commit()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        print ("Unable to delete these rows from the 'Movements_In_CartoDB' table.")
        print (rows)


def fetch_rows(connection, sql):
    rcursor = connection.cursor()
    try:
        rows = rcursor.execute(sql).fetchall()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        rows = None
    return rows


def get_locations_for_carto(connection, project):
    sql = ("select l.projectid, l.animalid, l.fixid, l.fixdate,"
           "location.Lat, Location.Long from locations as l "
           "left join ProjectExportBoundaries as b on b.Project = l.ProjectId "
           "left join Locations_In_CartoDB as c on l.fixid = c.fixid "
           "where c.FixId is null "  # not in CartoDB
           "and l.ProjectID = '" + project + "' "  # belongs to project
           "and l.[status] IS NULL "  # not hidden
           "and (b.shape is null or b.Shape.STContains(l.Location) = 1)")  # inside boundary
    return fetch_rows(connection, sql)


def get_vectors_for_carto(connection, project):
    sql = ("select m.Projectid, m.AnimalId, m.StartDate, m.EndDate, m.Duration, m.Distance, m.Speed, "
           "m.Shape.ToString() from movements as m "
           "inner join ProjectExportBoundaries as b on b.Project = m.ProjectId "
           "left join Movements_In_CartoDB as c "
           "on m.ProjectId = c.ProjectId and m.AnimalId = c.AnimalId "
           "and m.StartDate = c.StartDate and m.EndDate = c.EndDate "
           "where c.ProjectId IS NULL "  # not in CartoDB
           "and m.ProjectId = '" + project + "' "  # belongs to project
           "and Distance > 0 "  # not a degenerate
           "and (b.shape is null or b.Shape.STContains(m.shape) = 1)")  # inside boundary
    return fetch_rows(connection, sql)


def fixlocationrow(row):
    s = "('{0}','{1}',{2},'{3}',ST_SetSRID(ST_Point({5},{4}),4326))"
    return s.format(*row)


def fixmovementrow(row):
    s = "('{0}','{1}','{2}','{3}',{4},{5},{6},ST_GeometryFromText('{7}',4326))"
    return s.format(*row)


def insert(am, carto, lrows, vrows):
    if not lrows:
        print('No locations to send to CartoDB.')
    if not vrows:
        print('No movements to send to CartoDB.')
    if not lrows and not vrows:
        return
    if vrows:
        try:
            sql = ("insert into animal_movements "
                   "(projectid, animalid, startdate, enddate, duration, distance, speed, the_geom) values ")
            # Protection from really long lists, by executing multiple queries.
            for chunk in chunks(vrows, 900):
                values = ','.join([fixmovementrow(row) for row in chunk])
                # print(sql + values)
                carto.sql(sql + values)
            try:
                add_movements_to_carto_tracking_table(am, vrows)
                print('Wrote {0} movements to CartoDB.'.format(len(vrows)))
            except pyodbc.Error as de:
                print ("Database error ocurred", de)
        except CartoDBException as ce:
            print ("CartoDB error ocurred", ce)
    if lrows:
        try:
            sql = ("insert into animal_locations "
                   "(projectid,animalid,fixid,fixdate,the_geom) values ")
            ids, values = zip(*[(row[2], fixlocationrow(row)) for row in lrows])
            # Protection from really long lists, by executing multiple queries.
            for chunk in chunks(values, 900):
                vals = ','.join(chunk)
                # (sql + vals)
                carto.sql(sql + vals)
            try:
                add_locations_to_carto_tracking_table(am, ids)
                print('Wrote {0} locations to CartoDB.'.format(len(ids)))
            except pyodbc.Error as de:
                print ("Database error ocurred", de)
        except CartoDBException as ce:
            print ("CartoDB error ocurred", ce)


def get_locations_to_remove(connection):
    # check the list of location in Cartodb with the current status of locations
    sql = ("select c.fixid from Locations_In_CartoDB as c "
           "left join Locations as l on l.FixId = c.fixid "
           "left join ProjectExportBoundaries as b on b.Project = l.ProjectId "
           "where l.FixId is null "  # not in location table any longer
           "or l.status is not null "  # location is now hidden
           "or (b.shape is not null and b.shape.STContains(l.Location) = 0)")  # location is now outside boundary
    return fetch_rows(connection, sql)


def get_vectors_to_remove(connection):
    # check the list of movements in Cartodb with the current movements table
    # note: the attributes of a movement are immutable
    sql = ("select c.Projectid, c.AnimalId, c.StartDate, c.EndDate "
           "from Movements_In_CartoDB as c left join movements as m "
           "on m.ProjectId = c.ProjectId and m.AnimalId = c.AnimalId "
           "and m.StartDate = c.StartDate and m.EndDate = c.EndDate "
           "left join ProjectExportBoundaries as b on b.Project = m.ProjectId "
           "where m.projectid is null "  # not in movement database anylonger
           "or (b.shape is not null and b.shape.STContains(m.shape) = 0)")  # location is now outside boundary
    return fetch_rows(connection, sql)


def remove(am, carto, lrows, vrows):
    if not lrows:
        print('No locations to remove from CartoDB.')
    if not vrows:
        print('No movements to remove from CartoDB.')
    if not lrows and not vrows:
        return
    if vrows:
        try:
            sql = ("delete from animal_movements where "
                   "projectid = '{0}' and animalid = '{1}' and startdate = '{2}' and enddate = '{3}'")
            for row in vrows:
                sql1 = sql.format(row[0], row[1], row[2], row[3])
                carto.sql(sql1)
            try:
                remove_movements_from_carto_tracking_table(am, vrows)
                print('Removed {0} Movements from CartoDB.'.format(len(vrows)))
            except pyodbc.Error as de:
                print ("SQLServer error occurred.  Movements removed from CartoDB, but not SQLServer", de)
        except CartoDBException as ce:
            print ("CartoDB error occurred removing movements.", ce)
    if lrows:
        try:
            sql = "delete from animal_locations where fixid in "
            ids = [row[0] for row in lrows]
            # Protection from really long lists, by executing multiple queries.
            for chunk in chunks(ids, 900):
                idstr = '(' + ','.join(["{0}".format(i) for i in chunk]) + ')'
                carto.sql(sql + idstr)
            try:
                remove_locations_from_carto_tracking_table(am, ids)
                print('Removed {0} locations from CartoDB.'.format(len(ids)))
            except pyodbc.Error as de:
                print ("SQLServer error occurred.  Locations removed from CartoDB, but not SQLServer", de)
        except CartoDBException as ce:
            print ("CartoDB error occurred removing locations.", ce)


def fix_format_of_vector_columns(carto):
    sql = "update animal_movements set distance_t=round(cast(distance as numeric),1) where distance_t is null"
    execute_sql_in_cartodb(carto, sql)
    sql = "update animal_movements set duration_t=round(cast(duration as numeric),1) where duration_t is null"
    execute_sql_in_cartodb(carto, sql)
    sql = "update animal_movements set speed_t=round(cast(speed as numeric),1) where speed_t is null"
    execute_sql_in_cartodb(carto, sql)


def make_carto_tables():
    carto_conn = CartoDBAPIKey(secrets.apikey, secrets.domain)
    make_location_table_in_cartodb(carto_conn)
    make_movement_table_in_cartodb(carto_conn)


def make_sqlserver_tables():
    am_conn = get_connection_or_die()
    make_cartodb_tracking_tables(am_conn)


def main():
    carto_conn = CartoDBAPIKey(secrets.apikey, secrets.domain)
    am_conn = get_connection_or_die()
    locations = get_locations_to_remove(am_conn)
    vectors = get_vectors_to_remove(am_conn)
    remove(am_conn, carto_conn, locations, vectors)
    for project in ['KATM_BrownBear']:
        locations = get_locations_for_carto(am_conn, project)
        vectors = get_vectors_for_carto(am_conn, project)
        insert(am_conn, carto_conn, locations, vectors)
    fix_format_of_vector_columns(carto_conn)

if __name__ == '__main__':
    # make_carto_tables()
    # make_sqlserver_tables()
    main()
