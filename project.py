import mysql.connector
import sys

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
      
    except:
