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

    Usage:
       # Create a database session object
       db_sess = DBSession()

       # Connect to the database
       db_conn_attempt = db_sess.connect()

       # Determine if any connection errors occurred
       if db_conn_attempt["error"] == None:
           # no errors connecting, assign the connection object for use
           db_conn = db_conn_attempt["value"]
       else:
           # a connection error has occurred
           # ... handle the connection error...
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
        self.valEnvVarErr   = []        # env var validation errors
        self.valEnvVarsFlg  = False     # flag: validated env var values
        self.isConnectedFlg = False     # flag: successful db connection

        # Validate the database related environment variables
        self.val_env_vars()
            
        # Are there any validation errors?
        if len(self.valEnvVarErr) != 0:
            # one or more env vars are missing - return error message
            log.error("error: one or more invalid env var values: {err_str}".format(err_str="; ".join(self.valEnvVarErr)))
        else:
            # no validation errors found 
            self.valEnvVarsFlg = True
        
    def connect(self):
        """
        Connect to the Postgres database

        Returns a dictionary
          - "error": None if no connection error has occurred or an error message
          - "value": database connection object or None if an error has occurred
        """
        # Define a return dict
        ret_dict = {
            "error": None,
            "value": None
        }
        if self.isConnectedFlg:
            # Already connected to the database
            log.info("attempting to connect to the database but a connection already exists")
            ret_dict["value"] = self.dbconn
            return ret_dict
            
        if not self.valEnvVarsFlg:
            # Invalid database environment variables
            log.error("attempting to connect to the database one or more environment variables are invalid")
            ret_dict["error"] = "invalid environment variables"
            return ret_dict
            
        # Configure a Postgres database connection object
        self.dbconn = psycopg2.connect(user =        self.DS_DB_USER,
                                       password =    self.DS_DB_PASSWORD,
                                       host =        self.DS_DB_HOST,
                                       port =        self.DS_DB_PORT,
                                       database =    self.DS_DB_NAME)
                                   
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
            ret_dict["value"] = self.dbconn

        except (Exception, psycopg2.Error) as error:
            self.dbconn = None
            self.isConnectedFlg = False
            log.error("Error while connecting to PostgreSQL: {err}".format(err=error))
            ret_dict["error"] = "error: error connecting to the database: " + str(error)
            ret_dict["value"] = None
            
        return ret_dict
        
    def test_connection(self):
        """
        Test the instance's current database connection

        Returns a dictionary
          - "error": None if no connection error has occurred or an error message
          - "value": the database version or None if an error has occurred
        """
        # Define a return dict
        ret_dict = {
            "error": None,
            "value": None
        }
        # Test the database connection
        try:
            cursor = self.dbconn.cursor()

            # Print PostgreSQL version
            cursor.execute("SELECT version();")
            record = cursor.fetchone()
            log.info("Successfully connected to the database: {conn_details}\n".format(conn_details=record))
            self.isConnectedFlg = True
            ret_dict["value"] = record

        except (Exception, psycopg2.Error) as error:
            self.dbconn = None
            self.isConnectedFlg = False
            log.error("Error while connecting to PostgreSQL: {err}".format(err=error))
            ret_dict["error"] = "error connecting to the database: {err}".format(err=error)
            ret_dict["value"] = None
        
        return ret_dict

    def close_connection(self):
        """
        Close the instance's current database connection
        """
        if self.dbconn != None:
            self.dbconn.close()

        return

    # val_env_vars validates database related environment variables
    def val_env_vars(self):
        """
        val_env_vars validates the object's environment variable
        values that are used to drive the database connection
        and assigns errors to self.valEnvVarErr
        """
        # Validate the database connection environment variables
        if len(self.DS_DB_USER) == 0:
            # DS_DB_USER is missing
            self.valEnvVarErr.append("DS_DB_USER environment variable is missing")
            
        if len(self.DS_DB_PASSWORD) == 0:
            # DS_DB_PASSWORD is missing
            self.valEnvVarErr.append("DS_DB_PASSWORD environment variable is missing")
            
        if len(self.DS_DB_HOST) == 0:
            # DS_DB_HOST is missing
            self.valEnvVarErr.append("DS_DB_HOST environment variable is missing")
            
        if len(self.DS_DB_NAME) == 0:
            # DS_DB_NAME is missing
            self.valEnvVarErr.append("DS_DB_NAME environment variable is missing")
            
        if len(self.DS_DB_PORT) == 0:
            # DS_DB_PORT is missing
            self.valEnvVarErr.append("DS_DB_PORT environment variable is missing")

        return
