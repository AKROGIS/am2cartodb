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
api key - these are set in the `secrets.py` file.  See `testing_carto.py`
for a simple example, with explanations.

Third party requirements:
* carto - https://pypi.python.org/pypi/carto  (formerly cartodb)
* pyodbc - https://pypi.python.org/pypi/pyodbc - for SQL Server
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import sys

from cartodb import CartoDBAPIKey, CartoDBException
import pyodbc

import secrets


# Python 2/3 compatible xrange cabability
# pylint: disable=undefined-variable,redefined-builtin
if sys.version_info[0] < 3:
    range = xrange


def get_connection_or_die(server, database):
    """
    Get a Trusted pyodbc connection to the SQL Server database on server.

    Try several connection strings.
    See https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Windows

    Exit with an error message if there is no successful connection.
    """
    drivers = [
        "{ODBC Driver 17 for SQL Server}",  # supports SQL Server 2008 through 2017
        "{ODBC Driver 13.1 for SQL Server}",  # supports SQL Server 2008 through 2016
        "{ODBC Driver 13 for SQL Server}",  # supports SQL Server 2005 through 2016
        "{ODBC Driver 11 for SQL Server}",  # supports SQL Server 2005 through 2014
        "{SQL Server Native Client 11.0}",  # DEPRECATED: released with SQL Server 2012
        # '{SQL Server Native Client 10.0}',    # DEPRECATED: released with SQL Server 2008
    ]
    conn_template = "DRIVER={0};SERVER={1};DATABASE={2};Trusted_Connection=Yes;"
    for driver in drivers:
        conn_string = conn_template.format(driver, server, database)
        try:
            connection = pyodbc.connect(conn_string)
            return connection
        except pyodbc.Error:
            pass
    print("Rats!! Unable to connect to the database.")
    print("Make sure you have an ODBC driver installed for SQL Server")
    print("and your AD account has the proper DB permissions.")
    print("Contact akro_gis_helpdesk@nps.gov for assistance.")
    sys.exit()


def make_location_table_in_cartodb(carto):
    """Execute SQL on the carto server to create the Animal_Locations table."""

    sql = """
        CREATE TABLE Animal_Locations
        (ProjectId text NOT NULL, AnimalId text NOT NULL,
        FixDate timestamp NOT NULL, FixId int NOT NULL)
    """
    execute_sql_in_cartodb(carto, sql)
    sql = "select cdb_cartodbfytable('" + secrets.domain + "','Animal_Locations')"
    execute_sql_in_cartodb(carto, sql)


def make_movement_table_in_cartodb(carto):
    """Execute SQL on the carto server to create the Animal_Movements table."""

    sql = """
        CREATE TABLE Animal_Movements
        (ProjectId text NOT NULL, AnimalId text NOT NULL,
        StartDate timestamp NOT NULL, EndDate timestamp NOT NULL,
        Duration real NOT NULL, Distance real NOT NULL, Speed real NOT NULL,
        Duration_t text NULL, Distance_t text NULL, Speed_t text NULL)
    """
    execute_sql_in_cartodb(carto, sql)
    sql = "select cdb_cartodbfytable('" + secrets.domain + "','Animal_Movements')"
    execute_sql_in_cartodb(carto, sql)


def execute_sql_in_cartodb(carto, sql):
    """Execute SQL statement sql on carto server connection."""

    try:
        carto.sql(sql)
    except CartoDBException as ex:
        print("CartoDB error ocurred", ex)


def chunks(items, count):
    """Yield successive count-sized chunks from list items."""

    for i in range(0, len(items), count):
        yield items[i : i + count]


def make_cartodb_tracking_tables(connection):
    """Execute SQL to create tracking tables on the SQL Server connection."""

    sql = """
        if not exists (select * from sys.tables where name='Locations_In_CartoDB')
        create table Locations_In_CartoDB (fixid int NOT NULL PRIMARY KEY)
    """
    sql2 = """
        if not exists (select * from sys.tables where name='Movements_In_CartoDB')
          create table Movements_In_CartoDB (
            ProjectId varchar(16) NOT NULL,
            AnimalId varchar(16) NOT NULL,
            StartDate datetime2(7) NOT NULL,
            EndDate datetime2(7) NOT NULL
            CONSTRAINT PK_Movements_In_CartoDB PRIMARY KEY CLUSTERED (
              ProjectId ASC, AnimalId ASC, StartDate ASC, EndDate ASC))
    """
    w_cursor = connection.cursor()
    w_cursor.execute(sql)
    w_cursor.execute(sql2)
    try:
        w_cursor.commit()
    except pyodbc.Error as ex:
        print("Database error ocurred", ex)
        print("Unable to add create the 'Locations_In_CartoDB' table.")


def add_locations_to_carto_tracking_table(connection, fids):
    """Execute SQL to track location fids on the SQL Server connection."""

    # SQL Server is limited to 1000 rows in an insert
    w_cursor = connection.cursor()
    sql = "INSERT Locations_In_CartoDB (fixid) values "
    for chunk in chunks(fids, 900):
        values = ",".join(["({0})".format(fid) for fid in chunk])
        # print(sql + values)
        w_cursor.execute(sql + values)
    try:
        w_cursor.commit()
    except pyodbc.Error as ex:
        print("Database error ocurred", ex)
        print("Unable to add these ids to the 'Locations_In_CartoDB' table.")
        print(fids)


def add_movements_to_carto_tracking_table(connection, rows):
    """Execute SQL to track movement rows on the SQL Server connection."""

    # SQL Server is limited to 1000 rows in an insert
    w_cursor = connection.cursor()
    sql = """
        insert into Movements_In_CartoDB
        (projectid, animalid, startdate, enddate) values
    """
    for chunk in chunks(rows, 900):
        values = ",".join(["('{0}','{1}','{2}','{3}')".format(*row) for row in chunk])
        # print(sql + values)
        w_cursor.execute(sql + " " + values)
    try:
        w_cursor.commit()
    except pyodbc.Error as ex:
        print("Database error ocurred", ex)
        print("Unable to add these rows to the 'Movements_In_CartoDB' table.")
        print(rows)


def remove_locations_from_carto_tracking_table(connection, fids):
    """Execute SQL to un-track location fids on the SQL Server connection."""

    w_cursor = connection.cursor()
    sql = "delete from Locations_In_CartoDB where fixid in "
    # Protection from really long lists, by executing multiple queries.
    for chunk in chunks(fids, 900):
        ids = "(" + ",".join(["{0}".format(fid) for fid in chunk]) + ")"
        w_cursor.execute(sql + ids)
    try:
        w_cursor.commit()
    except pyodbc.Error as ex:
        print("Database error ocurred", ex)
        print("Unable to delete these ids from the 'Locations_In_CartoDB' table.")
        print(fids)


def remove_movements_from_carto_tracking_table(connection, rows):
    """Execute SQL to un-track movement rows on the SQL Server connection."""

    w_cursor = connection.cursor()
    sql = """
        delete from Movements_In_CartoDB where
        projectid = '{0}' and animalid = '{1}'
        and startdate = '{2}' and enddate = '{3}'
    """
    for row in rows:
        sql1 = sql.format(row[0], row[1], row[2], row[3])
        w_cursor.execute(sql1)
    try:
        w_cursor.commit()
    except pyodbc.Error as ex:
        print("Database error ocurred", ex)
        print("Unable to delete these rows from the 'Movements_In_CartoDB' table.")
        print(rows)


def fetch_rows(connection, sql):
    """Execute SQL statement sql on the SQL Server connection and return rows."""

    r_cursor = connection.cursor()
    try:
        rows = r_cursor.execute(sql).fetchall()
    except pyodbc.Error as ex:
        print("Database error ocurred", ex)
        rows = None
    return rows


def get_locations_for_carto(connection, project):
    """Return the new locations for project from the SQL Server connection."""

    sql = """
        select l.projectid, l.animalid, l.fixid, l.fixdate,
        location.Lat, Location.Long from locations as l
        left join ProjectExportBoundaries as b on b.Project = l.ProjectId
        left join Locations_In_CartoDB as c on l.fixid = c.fixid
        where c.FixId is null -- not in CartoDB
        and l.ProjectID = '{project}' -- belongs to project
        and l.[status] IS NULL -- not hidden
        and (b.shape is null or b.Shape.STContains(l.Location) = 1)
    """  # inside boundary
    return fetch_rows(connection, sql.format(project=project))


def get_vectors_for_carto(connection, project):
    """Return the new movements for project from the SQL Server connection."""

    sql = """
        select m.Projectid, m.AnimalId, m.StartDate, m.EndDate, m.Duration, m.Distance, m.Speed,
        m.Shape.ToString() from movements as m
        inner join ProjectExportBoundaries as b on b.Project = m.ProjectId
        left join Movements_In_CartoDB as c
        on m.ProjectId = c.ProjectId and m.AnimalId = c.AnimalId
        and m.StartDate = c.StartDate and m.EndDate = c.EndDate
        where c.ProjectId IS NULL  -- not in CartoDB
        and m.ProjectId = '{project}'  -- belongs to project
        and Distance > 0  -- not a degenerate
        and (b.shape is null or b.Shape.STContains(m.shape) = 1)
    """  # inside boundary
    return fetch_rows(connection, sql.format(project=project))


def fixlocationrow(row):
    """Return a modified location row; from SQL Server to Postgres (carto)."""

    text = "('{0}','{1}',{2},'{3}',ST_SetSRID(ST_Point({5},{4}),4326))"
    return text.format(*row)


def fixmovementrow(row):
    """Return a modified movement row; from SQL Server to Postgres (carto)."""

    text = "('{0}','{1}','{2}','{3}',{4},{5},{6},ST_GeometryFromText('{7}',4326))"
    return text.format(*row)


def insert(database, carto, l_rows, v_rows):
    """
    Send locations and movement vectors from connection to carto.

    locations (l_rows) and movement vectors (v_rows) will be marked as tracked
    on the source SQL Server connection and inserted on the tables on carto.
    """
    if not l_rows:
        print("No locations to send to CartoDB.")
    if not v_rows:
        print("No movements to send to CartoDB.")
    if not l_rows and not v_rows:
        return
    if v_rows:
        try:
            sql = """
                insert into animal_movements
                (projectid, animalid, startdate, enddate, duration,
                distance, speed, the_geom) values
            """
            # Protection from really long lists, by executing multiple queries.
            for chunk in chunks(v_rows, 900):
                values = ",".join([fixmovementrow(row) for row in chunk])
                # print(sql + values)
                carto.sql(sql + values)
            try:
                add_movements_to_carto_tracking_table(database, v_rows)
                print("Wrote {0} movements to CartoDB.".format(len(v_rows)))
            except pyodbc.Error as ex:
                print("Database error ocurred", ex)
        except CartoDBException as ex:
            print("CartoDB error ocurred", ex)
    if l_rows:
        try:
            sql = """
                insert into animal_locations
                (projectid,animalid,fixid,fixdate,the_geom) values
            """
            ids, values = zip(*[(row[2], fixlocationrow(row)) for row in l_rows])
            # Protection from really long lists, by executing multiple queries.
            for chunk in chunks(values, 900):
                values = ",".join(chunk)
                # (sql + values)
                carto.sql(sql + values)
            try:
                add_locations_to_carto_tracking_table(database, ids)
                print("Wrote {0} locations to CartoDB.".format(len(ids)))
            except pyodbc.Error as ex:
                print("Database error ocurred", ex)
        except CartoDBException as ex:
            print("CartoDB error ocurred", ex)


def get_locations_to_remove(connection):
    """
    Return the locations in SQL Server connection that should be removed from carto.

    Check the list of location in Carto with the current status of locations
    (hidden or deleted) or the boundary shape may have changed.
    """
    sql = """
        select c.fixid from Locations_In_CartoDB as c
        left join Locations as l on l.FixId = c.fixid
        left join ProjectExportBoundaries as b on b.Project = l.ProjectId
        where l.FixId is null -- not in location table any longer
        or l.status is not null -- location is now hidden
        or (b.shape is not null and b.shape.STContains(l.Location) = 0)
    """
    return fetch_rows(connection, sql)


def get_vectors_to_remove(connection):
    """
    Return the movements in SQL Server connection that should be removed from carto.

    Check the list of movements in Carto with the current status of movements
    (deleted) or the boundary shape may have changed. Note: the attributes of a
    movement are immutable, so we do not need to check them.
    """
    sql = """
        select c.Projectid, c.AnimalId, c.StartDate, c.EndDate
        from Movements_In_CartoDB as c left join movements as m
        on m.ProjectId = c.ProjectId and m.AnimalId = c.AnimalId
        and m.StartDate = c.StartDate and m.EndDate = c.EndDate
        left join ProjectExportBoundaries as b on b.Project = m.ProjectId
        where m.projectid is null -- not in movement database anylonger
        or (b.shape is not null and b.shape.STContains(m.shape) = 0)
    """
    return fetch_rows(connection, sql)


def remove(database, carto, l_rows, v_rows):
    """
    Remove locations and movement vectors from carto.

    locations (l_rows) and movement vectors (v_rows) will be marked as un-tracked
    on the source SQL Server connection and removed from the tables on carto.
    """
    if not l_rows:
        print("No locations to remove from CartoDB.")
    if not v_rows:
        print("No movements to remove from CartoDB.")
    if not l_rows and not v_rows:
        return
    if v_rows:
        try:
            sql = """
                delete from animal_movements where
                projectid = '{0}' and animalid = '{1}'
                and startdate = '{2}' and enddate = '{3}'
            """
            for row in v_rows:
                sql1 = sql.format(row[0], row[1], row[2], row[3])
                carto.sql(sql1)
            try:
                remove_movements_from_carto_tracking_table(database, v_rows)
                print("Removed {0} Movements from CartoDB.".format(len(v_rows)))
            except pyodbc.Error as ex:
                print(
                    "SQLServer error occurred.  Movements removed from CartoDB, but not SQLServer",
                    ex,
                )
        except CartoDBException as ex:
            print("CartoDB error occurred removing movements.", ex)
    if l_rows:
        try:
            sql = "delete from animal_locations where fixid in "
            ids = [row[0] for row in l_rows]
            # Protection from really long lists, by executing multiple queries.
            for chunk in chunks(ids, 900):
                id_str = "(" + ",".join(["{0}".format(i) for i in chunk]) + ")"
                carto.sql(sql + id_str)
            try:
                remove_locations_from_carto_tracking_table(database, ids)
                print("Removed {0} locations from CartoDB.".format(len(ids)))
            except pyodbc.Error as ex:
                print(
                    "SQLServer error occurred.  Locations removed from CartoDB, but not SQLServer",
                    ex,
                )
        except CartoDBException as ex:
            print("CartoDB error occurred removing locations.", ex)


def fix_format_of_vector_columns(carto):
    """Update the format of attributes in the movement table on carto."""

    sql = """
        update animal_movements
        set distance_t=round(cast(distance as numeric),1)
        where distance_t is null
    """
    execute_sql_in_cartodb(carto, sql)
    sql = """
        update animal_movements
        set duration_t=round(cast(duration as numeric),1)
        where duration_t is null
    """
    execute_sql_in_cartodb(carto, sql)
    sql = """
        update animal_movements
        set speed_t=round(cast(speed as numeric),1)
        where speed_t is null
    """
    execute_sql_in_cartodb(carto, sql)


def make_carto_tables():
    """Create the movement and location tables in Carto."""

    carto_conn = CartoDBAPIKey(secrets.apikey, secrets.domain)
    make_location_table_in_cartodb(carto_conn)
    make_movement_table_in_cartodb(carto_conn)


def make_sqlserver_tables():
    """Create the tracking tables in SQL Server."""

    am_conn = get_connection_or_die("inpakrovmais", "animal_movement")
    make_cartodb_tracking_tables(am_conn)


def main():
    """Update the Carto tables with changes in the Animal Movements tables."""

    carto_conn = CartoDBAPIKey(secrets.apikey, secrets.domain)
    am_conn = get_connection_or_die("inpakrovmais", "animal_movement")
    locations = get_locations_to_remove(am_conn)
    vectors = get_vectors_to_remove(am_conn)
    remove(am_conn, carto_conn, locations, vectors)
    for project in ["KATM_BrownBear"]:
        locations = get_locations_for_carto(am_conn, project)
        vectors = get_vectors_for_carto(am_conn, project)
        insert(am_conn, carto_conn, locations, vectors)
    fix_format_of_vector_columns(carto_conn)


if __name__ == "__main__":
    # make_carto_tables()
    # make_sqlserver_tables()
    main()
