import os
import logging

from dotenv import load_dotenv
import psycopg2

log = logging.getLogger(__name__)

class DBSession:
    """
    Define a DBSession class that handles connecting to a Postgres database
    and provides methods to test the connection and return a connection object
    for use
    """
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        # Fetch environment variable values
        self.DS_DB_USER     = os.getenv("DS_DB_USER")
        self.DS_DB_PASSWORD = os.getenv("DS_DB_PASSWORD")
        self.DS_DB_HOST     = os.getenv("DS_DB_HOST")
        self.DS_DB_NAME     = os.getenv("DS_DB_NAME")
        self.DS_DB_PORT     = os.getenv("DS_DB_PORT")
        # Set up other instance values
        self.dbconn         = None      # database connection object
        self.valEnvVarsFlg  = False     # flag: validated env var values
        self.valEnvVarsErr  = ""        # error: validated env var values
        self.isConnectedFlg = False     # flag: successful db connection
        
    def valEnvVars(self):
        """
        Validate the Postgres database connection env vars
        """
        errMsg = []
        if len(self.DS_DB_USER) == 0:
            # DS_DB_USER is missing
            errMsg.append("DS_DB_USER environment variable is missing")
            
        if len(self.DS_DB_PASSWORD) == 0:
            # DS_DB_PASSWORD is missing
            errMsg.append("DS_DB_PASSWORD environment variable is missing")
            
        if len(self.DS_DB_HOST) == 0:
            # DS_DB_HOST is missing
            errMsg.append("DS_DB_HOST environment variable is missing")
            
        if len(self.DS_DB_NAME) == 0:
            # DS_DB_NAME is missing
            errMsg.append("DS_DB_NAME environment variable is missing")
            
        if len(self.DS_DB_PORT) == 0:
            # DS_DB_PORT is missing
            errMsg.append("DS_DB_PORT environment variable is missing")
            
        if len(errMsg) != 0:
            # one or more env vars are missing - return error message
            self.valEnvVarsErr = "; ".join(errMsg)
            self.valEnvVarsFlg = False
            return self.valEnvVarsErr
            
        # No validation errors found - return None
        self.valEnvVarsFlg = True
        return None
        
    def connect(self):
        """
        Connect to the Postgres database
        """
        if self.isConnectedFlg:
            # Already connected to the database
            return "already connected to the database"
            
        if not self.valEnvVarsFlg:
            # Invalid database environment variables
            return "error: " + self.valEnvVarsErr
            
        # Configure a Postgres database connection object
        self.dbconn = psycopg2.connect(user =        self.DS_DB_USER,
                                       password =    self.DS_DB_PASSWORD,
                                       host =        self.DS_DB_HOST,
                                       port =        self.DS_DB_PORT,
                                       database =    self.DS_DB_NAME)
                                   
        # Test the database connection
        ret_str = ""
        try:
            cursor = self.dbconn.cursor()
            # Print PostgreSQL Connection properties
            log.info("Connecting to the database with these credentials: {conn_details}\n".format(conn_details=self.dbconn.get_dsn_parameters()))

            # Print PostgreSQL version
            cursor.execute("SELECT version();")
            record = cursor.fetchone()
            log.info("Successfully connected to the database: {conn_details}\n".format(conn_details=record))
            self.isConnectedFlg = True
            ret_str = record

        except (Exception, psycopg2.Error) as error:
            self.dbconn = None
            self.isConnectedFlg = False
            log.error("Error while connecting to PostgreSQL: {err}".format(err=error))
            ret_str = "error: error connecting to the database: " + str(error)
            
        return ret_str
        
    def get_connection(self):
        """
        Return the instance's Postgres database connection
        """
        if not self.isConnectedFlg:
            # No db connection exists
            log.error("attempting to return a db connection but no connection is active")
            return None
            
        return self.dbconn
        
    def test_connection(self):
        """
        Test the instance's current database connection
        """
        ret_str = ""
        # Test the database connection
        try:
            cursor = self.dbconn.cursor()
            # Print PostgreSQL Connection properties
            log.info("Connecting to the database with these credentials: {conn_details}\n".format(conn_details=self.dbconn.get_dsn_parameters()))

            # Print PostgreSQL version
            cursor.execute("SELECT version();")
            record = cursor.fetchone()
            log.info("Successfully connected to the database: {conn_details}\n".format(conn_details=record))
            self.isConnectedFlg = True
            ret_str = record

        except (Exception, psycopg2.Error) as error:
            self.dbconn = None
            self.isConnectedFlg = False
            ret_str = "Error while connecting to PostgreSQL: {err}".format(err=error)
            log.error(ret_str)
        
        return ret_str
