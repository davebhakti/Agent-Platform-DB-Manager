import mysql.connector
import sys
from pathlib import Path
import csv

DB_CONFIG = {
    'host': 'localhost',
    'user': 'test',
    'password': 'password',
    'database': 'cs122a',
}

# Connecting to the database:
def get_connected():
    try:
      conn = mysql.connector.connect(**DB_CONFIG)
      return conn
    except mysql.connector.Error as e:
      print(f"Error connecting to database: {e}")
      return None
  
def import_data(folder_name):
    try:
      conn = get_connected()
      if not conn:
        print("Failed to import due to no connection to Database")
        return
      cursor = conn.cursor()
      drop_tables = [
         "DROP TABLE IF EXISTS AgentClient",
         "DROP TABLE IF EXISTS AgentCreator",
         "DROP TABLE IF EXISTS BaseModel",
         "DROP TABLE IF EXISTS Configuration",
         "DROP TABLE IF EXISTS CustomizedModel",
         "DROP TABLE IF EXISTS DataStorage",
         "DROP TABLE IF EXISTS InternetService",
         "DROP TABLE IF EXISTS LLMService",
         "DROP TABLE IF EXISTS ModelConfigurations",
         "DROP TABLE IF EXISTS ModelServices",
         "DROP TABLE IF EXISTS User"
      ]
      for drop_command in drop_tables:
         cursor.execute(drop_command)
      create_tables = ["""CREATE TABLE User(
          UID VARCHAR(50) PRIMARY KEY,
          username VARCHAR(100) NOT NULL UNIQUE,
          email VARCHAR(100) NOT NULL
          )
          """,

         """CREATE TABLE AgentClient(
        UID VARCHAR(50) PRIMARY KEY,
        card_number VARCHAR(50) NOT NULL,
        cardholder_name VARCHAR(50) NOT NULL,
        expiration_date DATE NOT NULL,
        zip VARCHAR(10) NOT NULL,
        CVV VARCHAR(4) NOT NULL,
        interests TEXT
        FOREIGN KEY (UID) REFERENCES User(UID)
                ON DELETE CASCADE
        )
        """, 

        """
        CREATE TABLE AgentCreator(
        UID VARCHAR(50) PRIMARY KEY,
        bio TEXT,
        payout_account VARCHAR(50)
        FOREIGN KEY (UID) REFERENCES User(UID)
                ON DELETE CASCADE
        )
        """,

        """
        CREATE TABLE BaseModel(
        BMID VARCHAR(50) PRIMARY KEY,
        description TEXT
        creator_uid   VARCHAR(50) NOT NULL,
        FOREIGN KEY (creator_uid) REFERENCES AgentCreator(UID)
        )
        """,

        """
        CREATE TABLE CustomizedModel (
        BMID  INT NOT NULL,
        MID   INT NOT NULL,
        PRIMARY KEY (BMID, MID),
        FOREIGN KEY (BMID) REFERENCES BaseModel(BMID)
        ON DELETE CASCADE)
        """,

        """
        CREATE TABLE Configuration (
        CID       VARCHAR(50) PRIMARY KEY,
        client_uid VARCHAR(50) NOT NULL,
        label      VARCHAR(255),
        content    TEXT,
        FOREIGN KEY (client_uid) REFERENCES AgentClient(UID)
            ON DELETE CASCADE)
        """,

        """
        CREATE TABLE InternetService (
        sid       VARCHAR(50) PRIMARY KEY,
        provider  VARCHAR(255),
        endpoint  VARCHAR(500))
        """,

        """
        CREATE TABLE LLMService (
        sid     VARCHAR(50) PRIMARY KEY,
        domain  VARCHAR(255),
        FOREIGN KEY (sid) REFERENCES InternetService(sid)
            ON DELETE CASCADE
        """,

        """
        CREATE TABLE DataStorage (
        sid   VARCHAR(50) PRIMARY KEY,
        type  VARCHAR(255),
        FOREIGN KEY (sid) REFERENCES InternetService(sid)
            ON DELETE CASCADE)
        """,

        """
        CREATE TABLE ModelServices (
        BMID    VARCHAR(50) NOT NULL,
        sid      VARCHAR(50) NOT NULL,
        version  VARCHAR(50),
        PRIMARY KEY (BMID, sid),
        FOREIGN KEY (BMID) REFERENCES BaseModel(BMID)
            ON DELETE CASCADE,
        FOREIGN KEY (sid) REFERENCES InternetService(sid)
            ON DELETE CASCADE
        """,

        """
        CREATE TABLE ModelConfigurations (
        CID       VARCHAR(50) NOT NULL,
        BMID      VARCHAR(50) NOT NULL,
        MID       VARCHAR(50) NOT NULL,
        duration   INT,          
        PRIMARY KEY (CID, BMID, MID),
        FOREIGN KEY (CID) REFERENCES Configuration(CID)
            ON DELETE CASCADE,
        FOREIGN KEY (BMID, MID) REFERENCES CustomizedModel(BMID, MID)
            ON DELETE CASCADE)
        """
      ]

      for create_command in create_tables:
         cursor.execute(create_command)
      csv_files = {
        'User.csv': 'User',
        'AgentCreator.csv': 'AgentCreator',
        'AgentClient.csv': 'AgentClient',
        'Configuration.csv': 'Configuration',
        'BaseModel.csv': 'BaseModel',
        'CustomizedModel.csv': 'CustomizedModel',
        'InternetService.csv': 'InternetService',
        'LLMServie.csv': 'LLMService',
        'DataStorage.csv': 'DataStorage',
        'ModelServices.csv': 'ModelServices',
        'ModelConfigurations.csv': 'ModelConfigurations'
        }
      
      for csv_filename, table_name in csv_files.items():
         file_path = Path(folder_name) / csv_filename
         if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as filename:
               csv_reader = csv.reader(filename)
               next(csv_reader)
               for row in csv_reader:
                  row = [None if (val == '' or val == 'NULL') else val for val in row]
                  placeholders = ', '.join(['%s'] * len(row))
                  insert_stmt = f"INSERT INTO {table_name} VALUES ({placeholders})"
                  cursor.execute(insert_stmt, row)
      conn.commit()
      cursor.close()
      conn.close()

    except Exception as e:
      print("Fail")
      if conn:
        conn.rollback()