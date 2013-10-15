#!/usr/bin/env python

"""Datastore for PostgreSQL"""

__author__ = 'Michael Meisinger'

import inspect
import os.path
from uuid import uuid4
import simplejson as json

try:
    import psycopg2
    from psycopg2 import OperationalError, ProgrammingError, DatabaseError, IntegrityError, extensions
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    print "PostgreSQL driver not available!"

from pyon.core.exception import BadRequest, Conflict, NotFound, Inconsistent
from pyon.datastore.datastore_common import DataStore
from pyon.datastore.postgresql.pg_util import PostgresConnectionPool

from pyon.util.containers import get_ion_ts

from ooi.logging import log

MAX_STATEMENT_LOG = 2000


class PostgresDataStore(DataStore):
    """
    Base standalone datastore for PostgreSQL.
    Uses a connection pool to make greenlet safe concurrent database connections.
    """

    def __init__(self, datastore_name=None, config=None, scope=None, profile=None):
        """
        @param datastore_name  Name of datastore within server. May be scoped to sysname
        @param config  A server config dict with connection params
        @param scope  Prefix for the datastore name (e.g. sysname) to separate multiple systems
        """
        self.config = config
        if not self.config:
            self.config = {}

        # Connection basics
        self.host = self.config.get('host', 'localhost')
        self.username = self.config.get('username', "") or ""
        self.password = self.config.get('password', "") or ""
        self.database = self.config.get('database', 'ion')
        self.default_database = self.config.get('default_database', 'postgres')
        self.pool_maxsize = int(self.config.get('connection_pool_max', 5))

        self.profile = profile
        self.datastore_name = datastore_name

        self._datastore_cache = {}
        self._statement_log = []

        # Database (Postgres database) and datastore (database table) handling. Scope with
        # given scope (e.g. sysname) and make all lowercase for couch compatibility
        self.scope = scope
        if self.scope:
            self.database = "%s_%s" % (self.scope, self.database)
            self.datastore_name = ("%s_%s" % (self.scope, datastore_name)).lower() if datastore_name else None
        else:
            self.datastore_name = datastore_name.lower() if datastore_name else None

        self.pool = PostgresConnectionPool("dbname=%s user=%s password=%s" % (
            self.database, self.username, self.password), maxsize=self.pool_maxsize)
        try:
            with self.pool.connection() as conn:
                pass
        except OperationalError:
            log.info("Database '%s' does not exist", self.database)
            self._create_database(self.database)
            with self.pool.connection() as conn:
                # Check that connection works
                pass

        # Assert the existence of the datastore
        if self.datastore_name:
            if not self.datastore_exists():
                self.create_datastore()

        log.info("PostgresDataStore: created instance database=%s, datastore_name=%s, profile=%s, scope=%s",
                 self.database, self.datastore_name, self.profile, self.scope)

    def _create_database(self, database_name):
        log.info("Create database '%s'", database_name)
        conn = psycopg2.connect("dbname=%s" % self.default_database)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        try:
            cur.execute("CREATE DATABASE %s" % database_name)
            self._log_statement(cursor=cur)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        log.info("OK. Initialize database '%s'", database_name)
        conn2 = psycopg2.connect("dbname=%s" % database_name)
        cur = conn2.cursor()
        try:
            db_init = None
            with open("res/datastore/postgresql/db_init.sql", "r") as f:
                db_init = f.read()
            if db_init:
                cur.execute(db_init)
                self._log_statement(statement="EXECUTE db_init.sql")
            conn2.commit()
        finally:
            cur.close()
            conn2.close()
        log.debug("Database '%s' initialized and ready", database_name)

    def close(self):
        self.pool.closeall()

    @classmethod
    def drop_database(cls, database_name, default_database="postgres"):
        conn = psycopg2.connect("dbname=%s" % default_database)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        try:
            cur.execute("DROP DATABASE %s" % database_name)
            log.info("Dropped database '%s'", database_name)
        except Exception as ex:
            log.info("Could not drop database '%s'", database_name)
        conn.commit()
        cur.close()
        conn.close()


    # -------------------------------------------------------------------------
    # Couch database operations

    def _get_datastore_name(self, datastore_name=None):
        if datastore_name and self.scope:
            datastore_name = "%s_%s" % (self.scope, datastore_name)
        elif datastore_name:
            #datastore_name = datastore_name.lower()
            pass
        elif self.datastore_name:
            datastore_name = self.datastore_name
        else:
            raise BadRequest("No datastore name provided")

        if datastore_name != datastore_name.lower():
            # Compatibility with couch
            raise BadRequest("Invalid datastore name: %s" % datastore_name)

        return datastore_name

    def create_datastore(self, datastore_name=None, create_indexes=True, profile=None):
        """
        Create a datastore with the given name.  This is
        equivalent to creating a database on a database server.
        @param datastore_name  Datastore to work on. Will be scoped if scope was provided.
        @param create_indexes  If True create indexes according to profile
        @param profile  The profile used to determine indexes
        """
        ds_name = self._get_datastore_name(datastore_name)
        profile = profile or self.profile or "BASIC"
        log.info('Creating datastore %s (create_indexes=%s, profile=%s)' % (ds_name, create_indexes, profile))
        if profile == DataStore.DS_PROFILE.DIRECTORY:
            profile = DataStore.DS_PROFILE.RESOURCES

        profile = profile.lower()
        if not os.path.exists("res/datastore/postgresql/profile_%s.sql" % profile):
            profile = "basic"
        profile_sql = None
        with open("res/datastore/postgresql/profile_%s.sql" % profile, "r") as f:
            profile_sql = f.read()

        with self.pool.cursor() as cur:
            try:
                cur.execute(profile_sql % dict(ds=ds_name))
                self._log_statement(statement="EXECUTE profile_%s.sql" % profile)
            except ProgrammingError:
                raise BadRequest("Datastore with name %s already exists" % datastore_name)
            except DatabaseError as de:
                raise BadRequest("Datastore %s create error: %s" % (datastore_name, de))
            except Exception as de:
                raise BadRequest("Datastore %s create error: %s" % (datastore_name, de))
        log.debug("Datastore '%s' created" % (ds_name))

    def delete_datastore(self, datastore_name=None):
        """
        Delete the datastore with the given name.  This is
        equivalent to deleting a database from a database server.
        """
        if datastore_name is None:
            if self.datastore_name:
                datastore_name = self._get_datastore_name(datastore_name)
            else:
                raise BadRequest("Not datastore_name provided")
        elif not datastore_name.startswith(self.scope or ""):
            datastore_name = self._get_datastore_name(datastore_name)
        log.info('Deleting datastore %s' % datastore_name)

        with self.pool.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            log_entry = self._log_statement(cursor=cur)
            table_list = cur.fetchall()
            table_list = [e[0] for e in table_list]
            # print self.database, datastore_name, table_list

            table_del = 0
            for table in table_list:
                if table.startswith(datastore_name):
                    statement = "DROP TABLE "+table+" CASCADE"
                    cur.execute(statement)
                    # print self.database, statement, cur.rowcount
                    table_del += abs(cur.rowcount)

        log.debug("Datastore '%s' deleted (%s tables)" % (datastore_name, table_del))

        if datastore_name in self._datastore_cache:
            del self._datastore_cache[datastore_name]

    def clear_datastore(self, datastore_name=None):
        datastore_name = self._get_datastore_name(datastore_name)
        log.info('Clearing datastore %s' % datastore_name)

        with self.pool.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            self._log_statement(cursor=cur)
            table_list = cur.fetchall()
            table_list = [e[0] for e in table_list]

            table_del = 0
            for table in table_list:
                if table.startswith(datastore_name):
                    cur.execute("TRUNCATE TABLE "+table+" CASCADE")
                    self._log_statement(cursor=cur)
                    table_del += abs(cur.rowcount)

        log.debug("Datastore '%s' truncated (%s tables)" % (datastore_name, table_del))

        if datastore_name in self._datastore_cache:
            del self._datastore_cache[datastore_name]

    def list_datastores(self):
        with self.pool.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            self._log_statement(cursor=cur)
            table_list = cur.fetchall()
            table_list = [e[0] for e in table_list]

        datastore_list = []
        for ds in table_list:
            #if ds.startswith(self.scope):
            #    ds = ds[len(self.scope)+1:]
            if ds.endswith("_assoc") or ds.endswith("_att") or ds.endswith("_dir"):
                continue
            datastore_list.append(ds)
        log.debug("list_datastores(): %s", datastore_list)

        return datastore_list

    def info_datastore(self, datastore_name=None):
        datastore_name = self._get_datastore_name(datastore_name)
        return {}

    def compact_datastore(self, datastore_name=None):
        datastore_name = self._get_datastore_name(datastore_name)
        raise NotImplementedError()

    def datastore_exists(self, datastore_name=None):
        datastore_name = self._get_datastore_name(datastore_name)
        with self.pool.cursor() as cur:
            cur.execute("SELECT exists(SELECT * FROM information_schema.tables WHERE table_name=%s)", (datastore_name,))
            self._log_statement(cursor=cur)
            exists = cur.fetchone()[0]
            log.info("Datastore '%s' exists: %s", datastore_name, exists)

        return exists

    # -------------------------------------------------------------------------
    # Document operations

    def list_objects(self, datastore_name=None):
        """
        List all object types existing in the datastore instance.
        """
        datastore_name = self._get_datastore_name(datastore_name)
        with self.pool.cursor() as cur:
            cur.execute("SELECT id FROM "+datastore_name)
            self._log_statement(cursor=cur)
            id_list = cur.fetchall()
            id_list = [e[0] for e in id_list]

        return id_list

    def list_object_revisions(self, object_id, datastore_name=None):
        """
        Method for itemizing all the versions of a particular object
        known to the datastore.
        """
        datastore_name = self._get_datastore_name(datastore_name)
        return []

    def create_doc(self, doc, object_id=None, attachments=None, datastore_name=None):
        ds_name = datastore_name
        datastore_name = self._get_datastore_name(datastore_name)
        #log.debug('create_doc(): Create document id=%s', "id")

        with self.pool.cursor() as cur:
            try:
                oid, version = self._create_doc(cur, datastore_name, doc, object_id=object_id)
            except IntegrityError:
                raise BadRequest("Object with id %s already exists" % object_id)

        if attachments is not None:
            for att_name, att_value in attachments.iteritems():
                self.create_attachment(object_id, att_name, att_value['data'],
                                       content_type=att_value.get('content_type', ''), datastore_name=ds_name)

        return oid, version

    def create_doc_mult(self, docs, object_ids=None, datastore_name=None):
        if type(docs) is not list:
            raise BadRequest("Invalid type for docs:%s" % type(docs))
        if object_ids and len(object_ids) != len(docs):
            raise BadRequest("Invalid object_ids")
        if not docs:
            return []
        log.debug('create_doc_mult(): create %s documents', len(docs))

        datastore_name = self._get_datastore_name(datastore_name)
        # Could use cur.executemany() here but does not allow for case-by-case reaction to failure
        with self.pool.cursor() as cur:
            try:
                result_list = []
                for i, doc in enumerate(docs):
                    object_id = object_ids[i] if object_ids else None
                    try:
                        cur.execute("SAVEPOINT bulk_update")
                        oid, version = self._create_doc(cur, datastore_name, doc, object_id=object_id)
                    except IntegrityError:
                        log.warn("Doc exists, trying update id=%s", object_id)
                        cur.execute("ROLLBACK TO SAVEPOINT bulk_update")
                        oid, version = self._update_doc(cur, datastore_name, doc)
                    result_list.append((True, oid, version))
            except DatabaseError:
                raise

        return result_list

    def _create_doc(self, cur, table, doc, object_id=None):
        # Assign an id to doc
        if "_id" not in doc:
            object_id = object_id or self.get_unique_id()
            doc["_id"] = object_id

        doc["_rev"] = "1"
        doc_json = json.dumps(doc)

        extra_cols, table = self._get_extra_cols(doc, table, self.profile)

        statement_args = dict(id=doc["_id"], doc=doc_json)
        xcol, xval = "", ""
        if extra_cols:
            for col in extra_cols:
                value = doc.get(col, None)
                if value or type(value) is bool:
                    xcol += ", %s" % col
                    xval += ", %(" + col + ")s"
                    statement_args[col] = doc.get(col, None)

        statement = "INSERT INTO " + table + " (id, rev, doc" + xcol + ") VALUES (%(id)s, 1, %(doc)s" + xval + ")"
        cur.execute(statement, statement_args)
        self._log_statement(cursor=cur)
        return doc["_id"], "1"

    def create_attachment(self, doc, attachment_name, data, content_type=None, datastore_name=""):
        if not isinstance(attachment_name, str):
            raise BadRequest("attachment name is not string")
        if not isinstance(data, str) and not isinstance(data, file):
            raise BadRequest("data to create attachment is not a str or file")

        datastore_name = self._get_datastore_name(datastore_name)
        table = datastore_name + "_att"

        if isinstance(doc, str):
            doc_id = doc
        else:
            doc_id = doc['_id']
            self._assert_doc_rev(doc)

        statement_args = dict(docid=doc_id, rev=1, doc=buffer(data), name=attachment_name, content_type=content_type)
        with self.pool.cursor() as cur:
            statement = "INSERT INTO " + table + " (docid, rev, doc, name, content_type) "+\
                        "VALUES (%(docid)s, 1, %(doc)s, %(name)s, %(content_type)s)"
            try:
                cur.execute(statement, statement_args)
                self._log_statement(cursor=cur)
            except IntegrityError:
                raise NotFound('Object with id %s does not exist.' % doc_id)

    def update_doc(self, doc, datastore_name=None):
        if '_id' not in doc:
            raise BadRequest("Doc must have '_id'")
        if '_rev' not in doc:
            raise BadRequest("Doc must have '_rev'")
        datastore_name = self._get_datastore_name(datastore_name)
        #log.debug('update_doc(): Update document id=%s', doc['_id'])

        with self.pool.cursor() as cur:
            oid, version = self._update_doc(cur, datastore_name, doc)

        return oid, version

    def update_doc_mult(self, docs, datastore_name=None):
        if type(docs) is not list:
            raise BadRequest("Invalid type for docs:%s" % type(docs))
        if not all(["_id" in doc for doc in docs]):
            raise BadRequest("Docs must have '_id'")
        if not all(["_rev" in doc for doc in docs]):
            raise BadRequest("Docs must have '_rev'")
        if not docs:
            return []
        log.debug('update_doc_mult(): update %s documents', len(docs))

        datastore_name = self._get_datastore_name(datastore_name)
        # Could use cur.executemany() here but does not allow for case-by-case reaction to failure
        with self.pool.cursor() as cur:
            result_list = []
            for doc in docs:
                oid, version = self._update_doc(cur, datastore_name, doc)
                result_list.append((True, oid, version))

        return result_list

    def _update_doc(self, cur, table, doc):
        old_rev = int(doc["_rev"])
        doc["_rev"] = str(old_rev+1)
        doc_json = json.dumps(doc)

        extra_cols, table = self._get_extra_cols(doc, table, self.profile)

        statement_args = dict(doc=doc_json, id=doc["_id"], rev=old_rev, revn=old_rev+1)
        xval = ""
        if extra_cols:
            for col in extra_cols:
                value = doc.get(col, None)
                if value or type(value) is bool:
                    xval += ", " + col + "=%(" + col + ")s"
                    statement_args[col] = doc.get(col, None)

        cur.execute("UPDATE "+table+" SET doc=%(doc)s, rev=%(revn)s" + xval + " WHERE id=%(id)s AND rev=%(rev)s",
                    statement_args)
        self._log_statement(cursor=cur)
        if not cur.rowcount:
            # Distinguish rev conflict from documents does not exist.
            #try:
            #    self.read_doc(doc["_id"])
            #    raise Conflict("Object with id %s revision conflict" % doc["_id"])
            #except NotFound:
            #    raise
            raise Conflict("Object with id %s revision conflict" % doc["_id"])
        return doc["_id"], doc["_rev"]

    def _get_extra_cols(self, doc, table, profile):
        extra_cols = None
        if profile == DataStore.DS_PROFILE.RESOURCES:
            if doc.get("type_", None) == "Association":
                extra_cols = ["s", "st", "p", "o", "ot", "retired"]
                table = table + "_assoc"
            elif doc.get("type_", None):
                extra_cols = ["type_", "lcstate", "availability", "name", "ts_created"]
        elif profile == DataStore.DS_PROFILE.DIRECTORY:
            if doc.get("type_", None) == "DirEntry":
                extra_cols = ["org", "parent", "key"]
                table = table + "_dir"
        elif profile == DataStore.DS_PROFILE.EVENTS:
            if doc.get("origin", None):
                extra_cols = ["origin", "origin_type", "sub_type", "ts_created", "type_"]
        return extra_cols, table

    def update_attachment(self, doc, attachment_name, data, content_type=None, datastore_name=""):
        if not isinstance(attachment_name, str):
            raise BadRequest("attachment name is not string")
        if not isinstance(data, str) and not isinstance(data, file):
            raise BadRequest("data to create attachment is not a str or file")

        datastore_name = self._get_datastore_name(datastore_name)
        table = datastore_name + "_att"

        if isinstance(doc, str):
            doc_id = doc
        else:
            doc_id = doc['_id']
            self._assert_doc_rev(doc)

        statement_args = dict(docid=doc_id, rev=1, doc=buffer(data), name=attachment_name, content_type=content_type)
        with self.pool.cursor() as cur:
            statement = "UPDATE " + table + " SET "+\
                        "rev=rev+1, doc=%(doc)s,  content_type=%(content_type)s "+ \
                        "WHERE docid=%(docid)s AND name=%(name)s"
            cur.execute(statement, statement_args)
            self._log_statement(cursor=cur)
            if not cur.rowcount:
                raise NotFound('Attachment %s for object with id %s does not exist.' % (attachment_name, doc_id))

    def read_doc(self, doc_id, rev_id=None, datastore_name=None, object_type=None):
        datastore_name = self._get_datastore_name(datastore_name)

        if object_type == "Association":
            datastore_name = datastore_name + "_assoc"
        elif object_type == "DirEntry":
            datastore_name = datastore_name + "_dir"

        with self.pool.cursor() as cur:
            cur.execute("SELECT doc FROM "+datastore_name+" WHERE id=%s", (doc_id,))
            self._log_statement(cursor=cur)
            doc_list = cur.fetchall()
            if not doc_list:
                raise NotFound('Object with id %s does not exist.' % doc_id)
            if len(doc_list) > 1:
                raise Inconsistent('Object with id %s has %s values.' % (doc_id, len(doc_list)))

            doc_json = doc_list[0][0]
            #doc = json.loads(doc_json)
            doc = doc_json

        return doc

    def _read_doc_rev(self, doc_id, datastore_name=None):
        datastore_name = self._get_datastore_name(datastore_name)

        with self.pool.cursor() as cur:
            cur.execute("SELECT rev FROM "+datastore_name+" WHERE id=%s", (doc_id,))
            self._log_statement(cursor=cur)
            doc_list = cur.fetchall()
            if not doc_list:
                raise NotFound('Object with id %s does not exist.' % doc_id)

            rev = doc_list[0][0]

        return str(rev)

    def _assert_doc_rev(self, doc, datastore_name=None):
        rev = self._read_doc_rev(doc["_id"], datastore_name=datastore_name)
        if rev != doc["_rev"]:
            raise Conflict("Object with id %s revision conflict is=%s, need=%s" % (doc["_id"], rev, doc["_rev"]))

    def read_doc_mult(self, object_ids, datastore_name=None, object_type=None):
        """"
        Fetch a number of raw doc instances, HEAD rev.
        """
        if not object_ids:
            return []
        datastore_name = self._get_datastore_name(datastore_name)

        if object_type == "Association":
            datastore_name = datastore_name + "_assoc"
        elif object_type == "DirEntry":
            datastore_name = datastore_name + "_dir"

        query = "SELECT id, doc FROM "+datastore_name+" WHERE id IN ("
        query_args = dict()
        for i, oid in enumerate(object_ids):
            arg_name = "id" + str(i)
            if i>0:
                query += ","
            query += "%(" + arg_name + ")s"
            query_args[arg_name] = oid
        query += ")"

        with self.pool.cursor() as cur:
            cur.execute(query, query_args)
            self._log_statement(cursor=cur)
            rows = cur.fetchall()

        doc_by_id = {row[0]: row[1] for row in rows}
        doc_list = [doc_by_id.get(oid, None) for oid in object_ids]
        return doc_list

    def read_attachment(self, doc, attachment_name, datastore_name=""):
        datastore_name = self._get_datastore_name(datastore_name)
        table = datastore_name + "_att"

        doc_id = doc if isinstance(doc, str) else doc['_id']
        statement_args = dict(docid=doc_id, name=attachment_name)

        with self.pool.cursor() as cur:
            cur.execute("SELECT doc FROM "+table+" WHERE docid=%(docid)s AND name=%(name)s", statement_args)
            row = cur.fetchone()
            self._log_statement(cursor=cur, result=row)

        if not row:
            raise NotFound('Attachment %s does not exist in document %s.%s.',
                           attachment_name, datastore_name, doc_id)

        return str(row[0])

    def list_attachments(self, doc, datastore_name=""):
        datastore_name = self._get_datastore_name(datastore_name)
        table = datastore_name + "_att"

        doc_id = doc if isinstance(doc, str) else doc['_id']
        statement_args = dict(docid=doc_id)
        with self.pool.cursor() as cur:
            cur.execute("SELECT name, content_type FROM "+table+" WHERE docid=%(docid)s", statement_args)
            rows = cur.fetchall()
            self._log_statement(cursor=cur, result=rows)

        return [dict(name=row[0], content_type=row[1]) for row in rows]

    def delete_doc(self, doc, datastore_name=None, object_type=None, **kwargs):
        datastore_name = self._get_datastore_name(datastore_name)
        doc_id = doc if isinstance(doc, str) else doc["_id"]
        log.debug('delete_doc(): Delete document id=%s object_type=%s', doc_id, object_type)
        if self.profile == DataStore.DS_PROFILE.DIRECTORY:
            datastore_name = datastore_name + "_dir"
        if object_type == "Association":
            datastore_name = datastore_name + "_assoc"
        elif object_type == "DirEntry":
            datastore_name = datastore_name + "_dir"

        with self.pool.cursor() as cur:
            self._delete_doc(cur, datastore_name, doc_id)

    def delete_doc_mult(self, object_ids, datastore_name=None, object_type=None):
        if not object_ids:
            return []
        #log.debug('delete_doc_mult(): Delete %s documents', len(object_ids))
        datastore_name = self._get_datastore_name(datastore_name)
        if self.profile == DataStore.DS_PROFILE.DIRECTORY:
            datastore_name = datastore_name + "_dir"
        if object_type == "Association":
            datastore_name = datastore_name + "_assoc"
        elif object_type == "DirEntry":
            datastore_name = datastore_name + "_dir"

        with self.pool.cursor() as cur:
            for doc_id in object_ids:
                self._delete_doc(cur, datastore_name, doc_id)

    def _delete_doc(self, cur, table, doc_id):
        sql = "DELETE FROM "+table+" WHERE id=%s"
        cur.execute(sql, (doc_id, ))
        self._log_statement(cursor=cur)
        if not cur.rowcount:
            raise NotFound('Object with id %s does not exist.' % doc_id)

    def delete_attachment(self, doc, attachment_name, datastore_name=""):
        datastore_name = self._get_datastore_name(datastore_name)
        table = datastore_name + "_att"

        if isinstance(doc, str):
            doc_id = doc
        else:
            doc_id = doc['_id']
            self._assert_doc_rev(doc)

        statement_args = dict(docid=doc_id, name=attachment_name)
        with self.pool.cursor() as cur:
            cur.execute("DELETE FROM "+table+" WHERE docid=%(docid)s AND name=%(name)s", statement_args)
            self._log_statement(cursor=cur)
            if not cur.rowcount:
                raise NotFound('Attachment %s does not exist in document %s.%s.',
                               attachment_name, datastore_name, doc_id)

    # -------------------------------------------------------------------------
    # View operations

    def define_profile_views(self, profile=None, datastore_name=None, keepviews=False):
        pass

    def refresh_views(self, datastore_name="", profile=None):
        pass

    def _get_view_args(self, all_args):
        view_args = {}
        if all_args:
            view_args.update(all_args)
        extra_clause = ""
        if "limit" in all_args and all_args['limit'] > 0:
            extra_clause += " LIMIT %s" % all_args['limit']
        if "skip" in all_args and all_args['skip'] > 0:
            extra_clause += " OFFSET %s " % all_args['skip']

        view_args['extra_clause'] = extra_clause
        return view_args

    def find_docs_by_view(self, design_name, view_name, key=None, keys=None, start_key=None, end_key=None,
                          id_only=True, **kwargs):
        log.info("find_docs_by_view() %s/%s, %s, %s, %s, %s, %s, %s", design_name, view_name, key, keys, start_key, end_key, id_only, kwargs)

        funcname = "_find_%s" % (design_name) if view_name else "_find_all_docs"
        if not hasattr(self, funcname):
            raise NotImplementedError()

        filter = self._get_view_args(kwargs)

        res_list = getattr(self, funcname)(key=key, view_name=view_name, keys=keys, start_key=start_key, end_key=end_key, id_only=id_only, filter=filter)
        log.info("find_docs_by_view() found %s results", len(res_list))
        return res_list

    def _find_all_docs(self, view_name, key=None, keys=None, start_key=None, end_key=None,
                       id_only=True, filter=None):
        raise NotImplementedError()

    def _find_directory(self, view_name, key=None, keys=None, start_key=None, end_key=None,
                        id_only=True, filter=None):
        datastore_name = self._get_datastore_name()
        datastore_name = datastore_name + "_dir"
        query = "SELECT id, org, parent, key, doc FROM " + datastore_name
        query_clause = " WHERE "
        query_args = dict(key=key, start=start_key, end=end_key)

        if view_name == "by_key" and key:
            org = key[0]
            entry = key[1]
            parent = key[2]
            query_args.update(dict(org=org, parent=parent, key=entry))
            query_clause += "org=%(org)s AND parent=%(parent)s AND key=%(key)s"
        elif view_name == "by_key" and start_key:
            org = start_key[0]
            entry = start_key[1]
            parent = start_key[2]
            query_args.update(dict(org=org, parent="%s%%" % parent, key=entry))
            query_clause += "org=%(org)s AND parent LIKE %(parent)s AND key=%(key)s"
        elif view_name == "by_attribute":
            org = start_key[0]
            attr_name = start_key[1]
            attr_value = start_key[2]
            parent = start_key[3]
            query_args.update(dict(org=org, parent="%s%%" % parent, att="attributes.%s" % attr_name, val=attr_value))
            query_clause += "org=%(org)s AND parent LIKE %(parent)s AND json_string(doc,%(att)s)=%(val)s"
        elif view_name == "by_parent":
            org = start_key[0]
            parent = start_key[1]
            entry = start_key[2]
            query_args.update(dict(org=org, parent=parent, key=entry))
            query_clause += "org=%(org)s AND parent=%(parent)s"
        elif view_name == "by_path":
            org = start_key[0]
            parent = "/" + "/".join(start_key[1])
            query_args.update(dict(org=org, parent="%s%%" % parent))
            query_clause += "org=%(org)s AND parent LIKE %(parent)s"
        else:
        # by parent, path, attribute, key
            raise NotImplementedError()

        extra_clause = filter.get("extra_clause", "")
        with self.pool.cursor() as cur:
            #print query + query_clause + extra_clause, query_args
            cur.execute(query + query_clause + extra_clause, query_args)
            self._log_statement(cursor=cur)
            rows = cur.fetchall()

        #if view_name == "by_attribute":
        #    rows = [row for row in rows if row[2].startswith(start_key[3])]

        if id_only:
            res_rows = [(self._prep_id(row[0]), [], self._prep_doc(row[-1])) for row in rows]
        else:
            res_rows = [(self._prep_id(row[0]), [], self._prep_doc(row[-1])) for row in rows]

        return res_rows


    def _find_resource(self, view_name, key=None, keys=None, start_key=None, end_key=None,
                       id_only=True, filter=None):
        datastore_name = self._get_datastore_name()
        if id_only:
            query = "SELECT id, name, type_, lcstate FROM " + datastore_name
        else:
            query = "SELECT id, name, type_, lcstate, doc FROM " + datastore_name
        query_clause = " WHERE lcstate<>'RETIRED' AND "
        query_args = dict(key=key, start=start_key, end=end_key)

        if view_name == "by_type":
            query_args['type_'] = start_key[0]
            query_clause += "type_=%(type_)s"
        else:
            raise NotImplementedError()

        extra_clause = filter.get("extra_clause", "")
        with self.pool.cursor() as cur:
            cur.execute(query + query_clause + extra_clause, query_args)
            self._log_statement(cursor=cur)
            rows = cur.fetchall()

        if id_only:
            res_rows = [(self._prep_id(row[0]), [], None) for row in rows]
        else:
            res_rows = [(self._prep_id(row[0]), [], self._prep_doc(row[-1])) for row in rows]

        return res_rows

    def _find_attachment(self, view_name, key=None, keys=None, start_key=None, end_key=None,
                       id_only=True, filter=None):
        datastore_name = self._get_datastore_name()
        if id_only:
            query = "SELECT R.id, R.name, R.type_, R.lcstate, json_keywords(R.doc) FROM " + datastore_name + " AS R," + datastore_name + "_assoc AS A"
        else:
            query = "SELECT R.id, R.name, R.type_, R.lcstate, json_keywords(R.doc), R.doc FROM " + datastore_name + " AS R," + datastore_name + "_assoc AS A"
        query_clause = " WHERE R.id=A.o and A.p='hasAttachment' AND R.lcstate<>'RETIRED' AND A.retired<>true "
        query_args = dict(key=key, start=start_key, end=end_key)
        order_clause = " ORDER BY R.ts_created"

        if view_name == "by_resource":
            res_id = start_key[0]
            if len(start_key) > 1:
                raise NotImplementedError()
            query_args['resid'] = res_id
            query_clause += "AND A.s=%(resid)s"
        else:
            raise NotImplementedError()

        if filter.get('descending', False):
            order_clause += " DESC"

        extra_clause = filter.get("extra_clause", "")
        with self.pool.cursor() as cur:
            # print query + query_clause + order_clause + extra_clause, query_args
            cur.execute(query + query_clause + order_clause + extra_clause, query_args)
            self._log_statement(cursor=cur)
            rows = cur.fetchall()

        if id_only:
            res_rows = [(self._prep_id(row[0]), [None, None, row[4]], None) for row in rows]
        else:
            res_rows = [(self._prep_id(row[0]), [None, None, row[4]], self._prep_doc(row[-1])) for row in rows]

        return res_rows

    def _find_event(self, view_name, key=None, keys=None, start_key=None, end_key=None,
                    id_only=True, filter=None):
        datastore_name = self._get_datastore_name()
        if id_only:
            query = "SELECT id, ts_created FROM " + datastore_name
        else:
            query = "SELECT id, ts_created, doc FROM " + datastore_name
        query_clause = " WHERE "
        query_args = dict(key=key, start=start_key, end=end_key)
        order_clause = " ORDER BY ts_created"

        if view_name == "by_origintype":
            query_args['origin'] = start_key[0]
            query_args['type_'] = start_key[1]
            query_clause += "origin=%(origin)s AND type=%(type_)s"
            if len(start_key) == 3:
                query_args['startts'] = start_key[2]
                query_clause += " AND ts_created>=%(startts)s"
            if len(end_key) == 3:
                query_args['endts'] = end_key[2]
                query_clause += " AND ts_created<=%(endts)s"
            order_clause = " ORDER BY origin, type_, ts_created"
        elif view_name == "by_origin":
            query_args['origin'] = start_key[0]
            query_clause += "origin=%(origin)s"
            if len(start_key) == 2:
                query_args['startts'] = start_key[1]
                query_clause += " AND ts_created>=%(startts)s"
            if len(end_key) == 2:
                query_args['endts'] = end_key[1]
                query_clause += " AND ts_created<=%(endts)s"
            order_clause = " ORDER BY origin, ts_created"
        elif view_name == "by_type":
            query_args['type_'] = start_key[0]
            query_clause += "type_=%(type_)s"
            if len(start_key) == 2:
                query_args['startts'] = start_key[1]
                query_clause += " AND ts_created>=%(startts)s"
            if len(end_key) == 2:
                query_args['endts'] = end_key[1]
                query_clause += " AND ts_created<=%(endts)s"
            order_clause = " ORDER BY type_, ts_created"
        elif view_name == "by_time":
            if start_key and end_key:
                query_args['startts'] = start_key[0]
                query_args['endts'] = end_key[0]
                query_clause += "ts_created BETWEEN %(startts)s AND %(endts)s"
            elif start_key:
                query_args['startts'] = start_key[0]
                query_clause += "ts_created>=%(startts)s"
            elif end_key:
                query_args['endts'] = end_key[0]
                query_clause += "ts_created<=%(endts)s"
            else:
                # Make sure the result set is not too long
                if filter.get("limit", 0) < 0:
                    filter["limit"] = 100
                    filter = self._get_view_args(filter)
        else:
            raise NotImplementedError()

        if filter.get('descending', False):
            order_clause += " DESC"

        if query_clause == " WHERE ":
            query_clause = " "
        extra_clause = filter.get("extra_clause", "")
        with self.pool.cursor() as cur:
            sql = query + query_clause + order_clause + extra_clause
            #print "QUERY:", sql, query_args
            #print "filter:", filter
            cur.execute(sql, query_args)
            self._log_statement(cursor=cur)
            rows = cur.fetchall()

        if id_only:
            res_rows = [(self._prep_id(row[0]), [], row[1]) for row in rows]
        else:
            res_rows = [(self._prep_id(row[0]), [], self._prep_doc(row[-1])) for row in rows]

        return res_rows

    def get_unique_id(self):
        return uuid4().hex

    def _prep_id(self, internal_id):
        return internal_id.replace("-", "")

    def _prep_doc(self, internal_doc):
        # With latest psycopg2, this is not necessary anymore and can be removed
        if internal_doc is None:
            return None
        #doc = json.loads(internal_doc)
        doc = internal_doc
        return doc

    def _log_statement(self, context=None, statement=None, cursor=None, result=None, **kwargs):
        if not context:
            stack = inspect.stack()
            frame_num = 1
            context = ""
            while len(stack) > frame_num and frame_num < 6:
                context = "%s:%s:%s\n" % (stack[frame_num][1], stack[frame_num][2], stack[frame_num][3]) + context
                frame_num += 1
        rowcount = 0
        if cursor:
            statement = cursor.query
            rowcount = cursor.rowcount
        log_entry = dict(
            seq=0 if not self._statement_log else self._statement_log[0]['seq'] + 1,
            ts=get_ion_ts(),
            context=context,
            statement=statement,
            rowcount=rowcount
            #kwargs=kwargs
        )
        self._statement_log.insert(0, log_entry)
        if len(self._statement_log) > MAX_STATEMENT_LOG + 100:
            self._statement_log = self._statement_log[:MAX_STATEMENT_LOG]
        return log_entry

    def _log_query_results(self, log_entry, results):
        log_entry["statement_type"] = "query"
        log_entry["numrows"] = len(results)

    def _print_statement_log(self, max_log=10000):
        for i, log_entry in enumerate(self._statement_log[:max_log]):
            print "SQL %s @%s -> %s" % (log_entry['seq'], log_entry['ts'], log_entry['rowcount'])
            print log_entry['statement']
            print log_entry['context']

