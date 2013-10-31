This directory contains a fully functional PostgreSQL datastore implementation for the OOI Network.

REQUIREMENTS
- PostgreSQL 9.2.x or higher
- psycopg2 Python client 2.5 or higher (needs to do automatic JSON decoding)

ISSUES:
- Replace string concatenation when constructing statements
- support descending order for all finds
- create_mult is (ab)used for both create and update in one call by preload!
- list_datastore lists all tables, not just "datastores"
- tables are escaped by sysname scope as well (unnecessary)
- view on json_altids does not work
- drop database timeout does not raise exception

QUESTIONS:
- FILESYSTEM datastore used by preservation MS or not?
- Need to use priviledged user to create the database?
- EXPLAIN ANALYZE queries
- check query_view implementation (seems to be unused)
- Use postgres 9.3 json operators
- Rewrite some of the json functions

FUTURE FEATURES:
- history support by copying into separate history table
- specific views for resource types and associations
- Wrap cursor in factory to be able to instrument it (better than explicit log_statement)
- Consider SQLAlchemy
