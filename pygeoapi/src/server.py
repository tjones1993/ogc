import os
import tempfile
import psycopg2 

from src.utility import ps
from src.utility import fs


class Server:


    def __init__( self, obj ):

        """
        constructor
        """

        self._obj = obj
        self._default_port = 5432
        return


    def getHost( self ):

        """
        get host
        """

        return self._obj[ 'host' ]


    def getDatabase( self ):

        """
        get database
        """

        return self._obj[ 'database' ]


    def getPort( self ):

        """
        get port
        """

        return self._obj[ 'port' ] if 'port' in self._obj else self._default_port


    def getUser( self ):

        """
        get user
        """

        return self._obj[ 'user' ] if 'user' in self._obj else None


    def getPassword( self ):

        """
        get password
        """

        return self._obj[ 'password' ] if 'password' in self._obj else None


    def getGdalConnectionString( self ):

        """
        get password
        """

        # construct gdal connection string
        return "PG:host={host} port={port} dbname=\'{dbname}\' user=\'{user}\' password=\'{password}\'".format(   host=self.getHost(), 
                                                                                                                    port=self.getPort(), 
                                                                                                                    dbname=self.getDatabase(),
                                                                                                                    user=self.getUser(),
                                                                                                                    password=self.getPassword() )


    def getConnection( self ):

        """
        get psycopg arguments
        """

        # create connection string for psycopg
        cfg = "dbname='{}' host='{}'".format( self.getDatabase(), self.getHost() )

        # optional user
        if self.getUser() is not None:
            cfg += " user='{}'".format( self.getUser() )

        # optional password
        if self.getPassword() is not None:
            cfg += " password='{}'".format( self.getPassword() )

        # get connection
        return psycopg2.connect( cfg )


    def getRecords( self, query ):

        """
        execute query
        """

        records = []

        # get connection
        conn = self.getConnection()
        cur = conn.cursor()
        
        try:
            # execute query
            cur.execute( query )        
            records = cur.fetchall()

        # handle exception
        except psycopg2.Error as e:

            print ( e.pgerror )

        # close connection
        conn.close()
        return records


    def executeCommand( self, command, isolation_level=None ):

        """
        execute command
        """

        error = None

        # get connection
        conn = self.getConnection()

        if isolation_level is not None:
            conn.set_isolation_level(isolation_level)

        cur = conn.cursor()
        
        try:
            # execute query
            cur.execute( command )
            conn.commit()

        # handle exception
        except psycopg2.Error as e:

            print ( e.pgerror )
            error = e.pgerror

        # close connection
        conn.close()
        return error


    def getRecordCount( self, schema, table ):

        """
        execute query
        """

        count = None

        # get connection
        conn = self.getConnection()
        cur = conn.cursor()
        
        try:
            # execute query
            cur.execute( 'SELECT COUNT(*) FROM {schema}.{table}'.format( schema=schema, table=table ) )
            count = int ( cur.fetchone()[0] )

        # handle exception
        except psycopg2.Error as e:

            print ( e.pgerror )

        # close connection
        conn.close()
        return count


    def checkColumnExists( self, schema, table, column_name ):

        """
        check column exists in specified table
        """

        # construct boolean query of information_schema
        query = """
                SELECT EXISTS (SELECT 1 
                FROM information_schema.columns 
                WHERE table_schema='{schema}' AND table_name='{table}' AND UPPER(column_name)='{column_name}' );
                """.format ( schema=schema, table=table, column_name=column_name.upper()  )

        records = self.getRecords( query )
        return records[ 0 ][ 0 ]


    def checkTableExists( self, schema, table ):

        """
        check table object exists
        """

        # construct boolean query of information_schema
        query = """
                SELECT EXISTS (SELECT 1 
                FROM information_schema.tables
                WHERE table_schema='{schema}' AND table_name='{table}' );
                """.format ( schema=schema, table=table )

        records = self.getRecords( query )
        return records[ 0 ][ 0 ]


    def createOrReplaceTable( self, schema, table, fields ):

        """
        create or replace table 
        """

        # drop + create table
        self.dropTable( schema, table )
        return self.createTable( schema, table, fields )


    def dropTable( self, schema, table ):

        """
        drop table
        """

        # drop table if exists
        query = """
                DROP TABLE IF EXISTS {schema}.{table}
                """.format ( schema=schema, table=table )

        return self.executeCommand( query )


    def createTable( self, schema, table, fields ):

        """
        create table with fields arg
        """

        # create table with fields defined by arguments
        query = """
                CREATE TABLE IF NOT EXISTS {schema}.{table}
                """.format ( schema=schema, table=table )

        query += " ( id SERIAL PRIMARY KEY "
        for k, v in fields.items():
            query += ", {name} {type}".format( name=k, type=v )
        query += " )"

        # execute create table command
        return self.executeCommand( query )


    def checkSchemaExists( self, schema ):

        """
        check schema exists
        """

        # construct boolean query of information_schema
        query = """
                SELECT EXISTS (SELECT 1 
                FROM pg_catalog.pg_namespace 
                WHERE nspowner <> 1 AND nspname = '{schema}' );
                """.format ( schema=schema )

        records = self.getRecords( query )
        return records[ 0 ][ 0 ]


    def dropSchema( self, schema ):

        """
        drop schema
        """

        # drop schema if exists and everything it contains
        query = """
                DROP TABLE IF EXISTS {schema} CASCADE
                """.format ( schema=schema )

        return self.executeCommand( query )


    def createSchema( self, schema ):

        """
        create schema
        """

        # create table with fields defined by arguments
        query = """
                CREATE SCHEMA IF NOT EXISTS {schema}
                """.format ( schema=schema )

        # execute create table command
        return self.executeCommand( query )


    def getSchemaNames( self, match=None ):

        """
        get schemas
        """

        # get schema names
        query = """
                SELECT DISTINCT ( table_schema ) FROM information_schema.tables;
                """

        # add optional substring match
        if match is not None:
            query = """
                    SELECT DISTINCT ( table_schema ) FROM information_schema.tables WHERE table_schema ~ '{match}';
                    """.format( match=match )
        
        return self.getRecords( query )


    def getTableNames( self, schema, match=None ):

        """
        get table names
        """

        # get schema names
        query = """
                SELECT DISTINCT ( table_name ) FROM information_schema.tables WHERE table_schema='{schema}';
                """.format( schema=schema )
        
        # add optional substring match
        if match is not None:
            query = """
                    SELECT DISTINCT ( table_name ) FROM information_schema.tables WHERE table_schema='{schema}' AND table_name ~ '{match}';
                    """.format( schema=schema, match=match )

        return self.getRecords( query )


    def vacuumTables( self, schema, match=None ):

        """
        vacuum tables
        """

        # get table names 
        records = self.getTableNames( schema )
        for record in records:

            # apply optional match
            if match is None or match in record[ 0 ]:
                self.vacuumTable( schema, record[ 0 ] )

        return 


    def vacuumTable( self, schema, table ):

        """
        vacuum table
        """

        error = None

        # get table names
        records = self.getTableNames( schema )
        for record in records:

            # vacuum analyze table
            error = self.executeCommand(    """
                                            VACUUM ANALYZE {schema}.{table};
                                            """.format( schema=schema, table=record[ 0 ] ),
                                            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT )    
            # report error
            if error:
                print ( 'VACUUM ERROR: {schema}.{table}'.format( schema=schema, table=table ) )
                break
                                
        return error
