import mysql.connector
import sys
from pathlib import Path
import csv

DB_CONFIG = {
    "host": "localhost",
    "user": "test",
    "password": "password",
    "database": "cs122a"
}

def get_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except:
        return None

def import_data(folder_name):
    conn = get_connection()
    if conn is None:
        print("False")
        return False

    cursor = conn.cursor()

    drop_order = [
        "ModelConfigurations",
        "ModelServices",
        "DataStorage",
        "LLMService",
        "InternetService",
        "Configuration",
        "CustomizedModel",
        "BaseModel",
        "AgentClient",
        "AgentCreator",
        "User"
    ]

    for t in drop_order:
        cursor.execute(f"DROP TABLE IF EXISTS {t}")

    create_cmds = [

        """CREATE TABLE User (
            uid INT,
            email TEXT NOT NULL,
            username TEXT NOT NULL,
            PRIMARY KEY(uid)
        )""",

        """CREATE TABLE AgentCreator (
            uid INT,
            bio TEXT,
            payout TEXT,
            PRIMARY KEY(uid),
            FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
        )""",

        """CREATE TABLE AgentClient (
            uid INT,
            interests TEXT NOT NULL,
            cardholder TEXT NOT NULL,
            expire DATE NOT NULL,
            cardno INT NOT NULL,
            cvv INT NOT NULL,
            zip INT NOT NULL,
            PRIMARY KEY(uid),
            FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
        )""",

        """CREATE TABLE BaseModel (
            bmid INT,
            creator_uid INT NOT NULL,
            description TEXT NOT NULL,
            PRIMARY KEY(bmid),
            FOREIGN KEY (creator_uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
        )""",

        """CREATE TABLE CustomizedModel (
            bmid INT,
            mid INT NOT NULL,
            PRIMARY KEY(bmid, mid),
            FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
        )""",

        """CREATE TABLE Configuration (
            cid INT,
            client_uid INT NOT NULL,
            content TEXT NOT NULL,
            labels TEXT NOT NULL,
            PRIMARY KEY(cid),
            FOREIGN KEY (client_uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
        )""",

        """CREATE TABLE InternetService (
            sid INT,
            provider TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            PRIMARY KEY(sid)
        )""",

        """CREATE TABLE LLMService (
            sid INT,
            domain TEXT,
            PRIMARY KEY(sid),
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
        )""",

        """CREATE TABLE DataStorage (
            sid INT,
            type TEXT,
            PRIMARY KEY(sid),
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
        )""",

        """CREATE TABLE ModelServices (
            bmid INT NOT NULL,
            sid INT NOT NULL,
            version INT NOT NULL,
            PRIMARY KEY (bmid, sid),
            FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
        )""",

        """CREATE TABLE ModelConfigurations (
            bmid INT NOT NULL,
            mid INT NOT NULL,
            cid INT NOT NULL,
            duration INT NOT NULL,
            PRIMARY KEY (bmid, mid, cid),
            FOREIGN KEY (bmid, mid) REFERENCES CustomizedModel(bmid, mid) ON DELETE CASCADE,
            FOREIGN KEY (cid) REFERENCES Configuration(cid) ON DELETE CASCADE
        )"""
    ]

    for c in create_cmds:
        cursor.execute(c)

    csv_map = {
        "User.csv": "User",
        "AgentCreator.csv": "AgentCreator",
        "AgentClient.csv": "AgentClient",
        "BaseModel.csv": "BaseModel",
        "CustomizedModel.csv": "CustomizedModel",
        "Configuration.csv": "Configuration",
        "InternetService.csv": "InternetService",
        "LLMService.csv": "LLMService",
        "DataStorage.csv": "DataStorage",
        "ModelServices.csv": "ModelServices",
        "ModelConfigurations.csv": "ModelConfigurations",
    }

    for fname, tname in csv_map.items():
        path = Path(folder_name) / fname
        if path.exists():
            with open(path, "r") as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    placeholders = ",".join(["%s"] * len(row))
                    cursor.execute(f"INSERT INTO {tname} VALUES ({placeholders})", row)

    conn.commit()
    conn.close()
    print("True")
    return True

"""Question 2"""
def insert_agent_client(uid, username, email, cardno, cardholder, expire, cvv, zip_code, interests):
    conn = get_connection()
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO User VALUES (%s, %s, %s)", (uid, email, username))

        cursor.execute(
            """INSERT INTO AgentClient (uid, interests, cardholder, expire, cardno, cvv, zip)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (uid, interests, cardholder, expire, cardno, cvv, zip_code)
        )

        conn.commit()
        print("Success")
        return True
    except:
        if conn:
            conn.rollback()
        print("Fail")
        return False
    finally:
        if conn:
            conn.close()

"""Question 3"""
def add_customized_model(mid, bmid):
    conn = None
    try:
        conn = get_connection()
        if conn is None:
            print("Fail")
            return False

        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO CustomizedModel (bmid, mid) VALUES (%s, %s)",
            (bmid, mid)
        )

        conn.commit()
        print("Success")
        return True

    except:
        if conn:
            conn.rollback()
        print("Fail")
        return False
        
    finally:
        if conn:
            conn.close()


""" Question 4 """
def delete_base_model(bmid):
    conn = None
    try:
        conn = get_connection()
        if conn is None:
            print("Fail")
            return False

        cursor = conn.cursor()

        cursor.execute("DELETE FROM ModelServices WHERE bmid = %s", (bmid,))
        cursor.execute("DELETE FROM ModelConfigurations WHERE bmid = %s", (bmid,))
        cursor.execute("DELETE FROM CustomizedModel WHERE bmid = %s", (bmid,))
        cursor.execute("DELETE FROM BaseModel WHERE bmid = %s", (bmid,))

        # NEW treat it as a failure if basemodel cant delete
        if cursor.rowcount == 0:
            conn.rollback()
            print("Fail")
            return False

        conn.commit()
        print("Success")
        return True

    except:
        if conn:
            conn.rollback()
        print("Fail")
        return False
    finally:
        if conn:
            conn.close()

"""Question 5 """
def list_internet_service(bmid):
    conn = None
    try:
        conn = get_connection()
        if conn is None:
            return []

        cursor = conn.cursor()

        query = """
            SELECT i.sid, i.endpoints, i.provider
            FROM ModelServices ms
            JOIN InternetService i ON ms.sid = i.sid
            WHERE ms.bmid = %s
            ORDER BY i.provider ASC
        """

        cursor.execute(query, (bmid,))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
            return False
        for row in rows:
            sid = int(row[0])
            endpoint = str(row[1])
            provider = str(row[2])
            print(f"{sid},{endpoint},{provider}")
        return True
    except:
        print("Fail")
        return False
    finally:
        conn.close()

""" Question 6 """
def count_customized_model(*bmid_list):
    if len(bmid_list) == 1 and isinstance(bmid_list[0], list):
        bmid_list = bmid_list[0]

    conn = get_connection()
    if conn is None:
        return []

    try:
        cursor = conn.cursor()

        placeholders = ",".join(["%s"] * len(bmid_list))

        query = f"""
            SELECT b.bmid, b.description, COUNT(c.mid) AS customizedModelCount
            FROM BaseModel b
            LEFT JOIN CustomizedModel c ON b.bmid = c.bmid
            WHERE b.bmid IN ({placeholders})
            GROUP BY b.bmid
            ORDER BY b.bmid ASC
        """

        cursor.execute(query, bmid_list)
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
            return False
        for row in rows:
            print(f"{row[0]},{row[1]},{row[2]}")
        return True
    except:
        print("Fail")
        return False
    finally:
        conn.close()

"""Question 7"""
def topN_duration_config(uid, N):
    conn = get_connection()
    if conn is None:
        return []

    try:
        cursor = conn.cursor()

        query = """
            SELECT c.client_uid, mc.cid, c.labels, c.content, mc.duration
            FROM ModelConfigurations mc
            JOIN Configuration c ON mc.cid = c.cid
            WHERE c.client_uid = %s
            ORDER BY mc.duration DESC
            LIMIT %s
        """

        cursor.execute(query, (uid, N))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
            return False
        for row in rows:
            print(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}")
        return True
    except:
        print("Fail")
        return False
    finally:
        conn.close()

"""Question 8"""
def listBaseModelKeyword(keyword):
    conn = get_connection()
    if conn is None:
        return

    try:
        cursor = conn.cursor()

        query = """
            SELECT DISTINCT b.bmid, l.sid, i.provider, l.domain
            FROM BaseModel b
            JOIN ModelServices ms ON b.bmid = ms.bmid
            JOIN LLMService l ON ms.sid = l.sid
            JOIN InternetService i ON l.sid = i.sid
            WHERE l.domain LIKE %s
            ORDER BY b.bmid ASC
            LIMIT 5
        """

        cursor.execute(query, (f"%{keyword}%",))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
            return False
        for row in rows:
            print(f"{row[0]},{row[1]},{row[2]},{row[3]}")
        return True
    except:
        print("Fail")
        return False
    finally:
        conn.close()
