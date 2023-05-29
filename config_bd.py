
import psycopg2
import os

config = {
            "host":'localhost',
            "port":'5432',
            "database":'maille_commune',
            "user":'postgres',
            "password":'admin'
        }



from configparser import ConfigParser
import psycopg2
import os
from sqlalchemy import create_engine


db_uri = "postgresql://postgres:admin@localhost:5432/maille_commune"

class engine_conn(object):
    """
    Context manager pour garantir la fermeture de la connexion à la base de données
    """
    def __init__(self):
        self.conn = None
    def __enter__(self):
        # Specify the connection details in a SQLAlchemy URL format

        # Create a SQLAlchemy engine to connect to the PostgreSQL database
        engine = create_engine(db_uri)
        self.conn = engine.connect()
        return self.conn


    def __exit__(self, type, value, traceback):
        self.conn.close()
class bdd_connection(object):
    """
    Context manager pour garantir la fermeture de la connexion à la base de données
    """
    def __init__(self):
        self.conn = None
    def __enter__(self):
        self.conn = psycopg2.connect(**config)
        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.close()

