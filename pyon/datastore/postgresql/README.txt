This directory contains a fully functional PostgreSQL datastore implementation for ION.

REQUIREMENTS
- postgresql 9.2.x or higher
- psycopg2 Python client

ISSUES:
- resource finds exclude resources in RETIRED state
- support descending order for all finds
- query_view implementation (seems to be unused)
- create_mult is (ab)used for both create and update in one call by preload!
- list_datastore lists all tables, not just "datastores"
- tables are escaped by scope as well (unnecessary)
- view on json_altids does not work

QUESTIONS:
- FILESYSTEM datastore used by preservation MS or not?
- Check unicode result JSON read
- priviledged user to create the database?
- EXPLAIN ANALYZE queries

FUTURE FEATURES:
- history support by copying into separate history table
