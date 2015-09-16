import sys
import secrets

__author__ = 'RESarwas'

# dependency pyodbc
# C:\Python27\ArcGIS10.3\Scripts\pip.exe install pyodbc
# dependency cartodb
# C:\Python27\ArcGIS10.3\Scripts\pip.exe install cartodb


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
    for i in xrange(0, len(l), n):
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


def add_movements_to_carto_tracking_table(connection, rows):
    # SQL Server is limited to 1000 rows in an insert
    wcursor = connection.cursor()
    sql = "insert into Movements_In_CartoDB (projectid, animalid, startdate, enddate) values "
    for chunk in chunks(rows, 900):
        values = ','.join(["('{0}','{1}','{2}','{3}')".format(*row) for row in chunk])
        # print sql + values
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
    ids = '(' + ','.join([str(i) for i in fids]) + ')'
    wcursor.execute(sql+ids)
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
    sql = ("select l.projectid,l.animalid,l.fixid,l.fixdate,"
           "location.Lat,Location.Long from locations as l "
           "inner join ProjectExportBoundaries as b on l.Location.STIntersects(b.Shape) = 1 "
           "left join Locations_In_CartoDB as c on l.fixid = c.fixid "
           "where l.ProjectID = '" + project + "' and c.fixid IS NULL and l.[status] IS NULL")
    return fetch_rows(connection, sql)


def get_vectors_for_carto(connection, project):
    sql = ("select m.Projectid, m.AnimalId, m.StartDate, m.EndDate, m.Duration, m.Distance, m.Speed, "
           "m.Shape.ToString() from movements as m "
           "inner join ProjectExportBoundaries as b on m.Shape.STIntersects(b.Shape) = 1 "
           "left join Movements_In_CartoDB as c "
           "on m.ProjectId = c.ProjectId and m.AnimalId = c.AnimalId "
           "and m.StartDate = c.StartDate and m.EndDate = c.EndDate "
           "where m.ProjectId = '" + project + "' and c.ProjectId IS NULL and Distance > 0")
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
            values = ','.join([fixmovementrow(row) for row in vrows])
            # print sql + values
            carto.sql(sql + values)
            try:
                add_movements_to_carto_tracking_table(am, vrows)
                print('Wrote ' + str(len(vrows)) + ' movements to CartoDB.')
            except pyodbc.Error as de:
                print ("Database error ocurred", de)
        except CartoDBException as ce:
            print ("CartoDB error ocurred", ce)
    if lrows:
        try:
            sql = ("insert into animal_locations "
                   "(projectid,animalid,fixid,fixdate,the_geom) values ")
            ids, values = zip(*[(row[2], fixlocationrow(row)) for row in lrows])
            values = ','.join(values)
            # print sql + values
            carto.sql(sql + values)
            try:
                add_locations_to_carto_tracking_table(am, ids)
                print('Wrote ' + str(len(ids)) + ' locations to CartoDB.')
            except pyodbc.Error as de:
                print ("Database error ocurred", de)
        except CartoDBException as ce:
            print ("CartoDB error ocurred", ce)


def get_locations_to_remove(connection, project):
    # status is the only mutable field in the locations table, so that is all we need to check
    # if the boundary changes, then clear then kill and fill
    sql = ("select l.fixid from locations as l "
           "left join Locations_In_CartoDB as c on l.fixid = c.fixid "
           "where ProjectID = '" + project + "' and c.fixid IS NOT NULL "
           "and [status] IS NOT NULL")
    return fetch_rows(connection, sql)


def get_vectors_to_remove(connection, project):
    # movement records are immutable, however changes to location status add/delete records
    # if the boundary changes, then clear then kill and fill
    sql = ("select c.Projectid, c.AnimalId, c.StartDate, c.EndDate "
           "from Movements_In_CartoDB as c left join movements as m "
           "on m.ProjectId = c.ProjectId and m.AnimalId = c.AnimalId "
           "and m.StartDate = c.StartDate and m.EndDate = c.EndDate "
           "where c.ProjectId = '" + project + "' and m.projectid is null")
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
                print('Removed ' + str(len(vrows)) + ' Movements from CartoDB.')
            except pyodbc.Error as de:
                print ("Database error ocurred", de)
        except CartoDBException as ce:
            print ("CartoDB error ocurred", ce)
    if lrows:
        try:
            sql = "delete from animal_locations where fixid in "
            ids = [row[0] for row in lrows]
            idstr = '(' + ','.join([str(i) for i in ids]) + ')'
            carto.sql(sql + idstr)
            try:
                remove_locations_from_carto_tracking_table(am, ids)
                print('Removed ' + str(len(ids)) + ' locations from CartoDB.')
            except pyodbc.Error as de:
                print ("Database error ocurred", de)
        except CartoDBException as ce:
            print ("CartoDB error ocurred", ce)


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
    for project in ['KATM_BrownBear']:
        locations = get_locations_to_remove(am_conn, project)
        vectors = get_vectors_to_remove(am_conn, project)
        remove(am_conn, carto_conn, locations, vectors)
        locations = get_locations_for_carto(am_conn, project)
        vectors = get_vectors_for_carto(am_conn, project)
        insert(am_conn, carto_conn, locations, vectors)
    fix_format_of_vector_columns(carto_conn)

if __name__ == '__main__':
    # make_carto_tables()
    # make_sqlserver_tables()
    main()
