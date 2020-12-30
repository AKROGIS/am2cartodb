# Animal Movements to CartoDB

A python tool for publishing select data from the
[Animal Movement](https://github.com/AKROGIS/AnimalMovement)
database (an internal SQL Server) to public
[CartoDB](https://cartodb.com)
services.  The tool keeps track of what data has already been
published, and only pushes changes.  It is best run as a
scheduled task.

Biologists with data in Animal Movements must elect to publish
their data in cartodb.  Currently only the Katmai Bear data is
being published. Locations are filtered by project and location
(if an animal goes outside the protection of the park, its
location is not published).

The Services are at
https://nationalparkservice.carto.com/u/nps-akro-gis.

## Build

The code is run with Python 2.7.x. It depends on the
[pyodbc module](https://pypi.org/project/pyodbc/)
and the
[cartodb module](https://pypi.org/project/cartodb/).
These can be installed with `pip install pyodbc` and
`pip install cartodb`.

Copy the `secrets.py.example` file to `secrets.py` and
edit with your cartodb account and an API Key.
AKRO GIS staff can find these and a completed
`secrets.py` file in the password keeper on the
team network drive.

## Deploy

The code requires tables be created in the source database
and on the Cartodb server.  Those tables have already been
created for the current configuration.  (Using the commented out
[first two lines](https://github.com/regan-sarwas/am2cartodb/blob/55be163f56805e5ca0f063dcfa8ba3c350895f90/upload.py#L345-L346)
of the main function in `upload.py`.)
If this code is being used in a new configuration, it will
require a lot of modification, as the existing DB schema is
hard coded throughout the file.

## Using

Once the tables have been created, the `upload.py` script can
be run as often as desired.  It is running daily as a scheduled
task on the AKRO GIS servers.  It needs to be run with an
account that has viewer privileges in the source database.
