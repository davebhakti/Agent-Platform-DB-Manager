import mysql.connector
import csv
from pathlib import Path
import sys

DB_CONFIG = {
    "host": "localhost",
    "user": "test",
    "password": "password",
    "database": "cs122a",
}

# ------------------------------
# Database connection
# ------------------------------
def get_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except:
        return None

# ------------------------------
# Question 1: Import Data
# ------------------------------
def import_data(folder_name):
    conn = get_connection()
    if conn is None:
        print("Fail")
    try:
        cursor = conn.cursor()
        # Drop tables in dependency order
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

        # Create tables
        create_cmds = [
            """CREATE TABLE User (
                uid INT PRIMARY KEY,
                email TEXT NOT NULL,
                username TEXT NOT NULL
            )""",
            """CREATE TABLE AgentCreator (
                uid INT PRIMARY KEY,
                bio TEXT,
                payout TEXT,
                FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE AgentClient (
                uid INT PRIMARY KEY,
                interests TEXT NOT NULL,
                cardholder TEXT NOT NULL,
                expire DATE NOT NULL,
                cardno BIGINT NOT NULL,
                cvv INT NOT NULL,
                zip INT NOT NULL,
                FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE BaseModel (
                bmid INT PRIMARY KEY,
                creator_uid INT NOT NULL,
                description TEXT NOT NULL,
                FOREIGN KEY (creator_uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE CustomizedModel (
                bmid INT,
                mid INT NOT NULL,
                PRIMARY KEY(bmid, mid),
                FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
            )""",
            """CREATE TABLE Configuration (
                cid INT PRIMARY KEY,
                client_uid INT NOT NULL,
                content TEXT NOT NULL,
                labels TEXT NOT NULL,
                FOREIGN KEY (client_uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE InternetService (
                sid INT PRIMARY KEY,
                provider TEXT NOT NULL,
                endpoint TEXT NOT NULL
            )""",
            """CREATE TABLE LLMService (
                sid INT PRIMARY KEY,
                domain TEXT,
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE DataStorage (
                sid INT PRIMARY KEY,
                type TEXT,
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE ModelServices (
                bmid INT NOT NULL,
                sid INT NOT NULL,
                version INT NOT NULL,
                PRIMARY KEY(bmid, sid),
                FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE ModelConfigurations (
                bmid INT NOT NULL,
                mid INT NOT NULL,
                cid INT NOT NULL,
                duration INT NOT NULL,
                PRIMARY KEY(bmid, mid, cid),
                FOREIGN KEY(bmid, mid) REFERENCES CustomizedModel(bmid, mid) ON DELETE CASCADE,
                FOREIGN KEY(cid) REFERENCES Configuration(cid) ON DELETE CASCADE
            )"""
        ]
        for c in create_cmds:
            cursor.execute(c)
        # Map CSV files to table names
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
            "ModelConfigurations.csv": "ModelConfigurations"
        }

        # Insert CSV data
        for fname, tname in csv_map.items():
            path = Path(folder_name) / fname
            if not path.exists():
                print("Fail")
                return
            
            with open(path, "r") as f:
                reader = csv.reader(f)
                next(reader)  # skip header
                for row in reader:
                    # Process each value
                    processed_row = []
                    for i, cell in enumerate(row):
                        if cell == "NULL":
                            processed_row.append(None)
                        else:
                            # Convert to appropriate type based on table and column
                            if tname == "User" and i == 0:  # uid
                                processed_row.append(int(cell))
                            elif tname == "AgentCreator" and i == 0:  # uid
                                processed_row.append(int(cell))
                                
                            elif tname == "AgentClient":
                                if i == 0:  # uid
                                    processed_row.append(int(cell))
                                elif i == 4:  # cardno
                                    processed_row.append(int(cell))
                                elif i == 5:  # cvv
                                    processed_row.append(int(cell))
                                elif i == 6:  # zip
                                    processed_row.append(int(cell))
                                else:
                                    processed_row.append(cell)
                            elif tname == "BaseModel":
                                if i == 0:  # bmid
                                    processed_row.append(int(cell))
                                elif i == 1:  # creator_uid
                                    processed_row.append(int(cell))
                                else:
                                    processed_row.append(cell)
                            elif tname == "CustomizedModel":
                                processed_row.append(int(cell))  # both bmid and mid are ints
                            elif tname == "Configuration":
                                if i == 0:  # cid
                                    processed_row.append(int(cell))
                                elif i == 1:  # client_uid
                                    processed_row.append(int(cell))
                                else:
                                    processed_row.append(cell)
                            elif tname == "InternetService" or tname == "LLMService" or tname == "DataStorage":
                                if i == 0:  # sid
                                    processed_row.append(int(cell))
                                else:
                                    processed_row.append(cell)
                            elif tname == "ModelServices":
                                processed_row.append(int(cell))  # all columns are ints
                            elif tname == "ModelConfigurations":
                                processed_row.append(int(cell))  # all columns are ints
                            else:
                                processed_row.append(cell)
                    
                    placeholders = ",".join(["%s"] * len(processed_row))
                    cursor.execute(f"INSERT INTO {tname} VALUES ({placeholders})", processed_row)

        conn.commit()
        print("Success")
    except Exception as e:
        if conn:
            conn.rollback()
        print("Fail" )
    finally:
        if conn:
            conn.close()

# ------------------------------
# Question 2: Insert Agent Client
# ------------------------------
def insertAgentClient(uid, username, email, cardno, cardholder, expire, cvv, zip_code, interests):
    conn = get_connection()
    if conn is None:
        print("Fail")
        return
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO User (uid, email, username) VALUES (%s, %s, %s)",
            (uid, email, username)
        )

        cursor.execute(
            "INSERT INTO AgentClient (uid, interests, cardholder, expire, cardno, cvv, zip) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (uid, interests, cardholder, expire, cardno, cvv, zip_code)
        )
        conn.commit()
        print("True")
    except Exception as e:
        if conn:
            conn.rollback()
        print("Fail")
    finally:
        if conn:
            conn.close()

# ------------------------------
# Question 3: Add Customized Model
# ------------------------------
def addCustomizedModel(mid, bmid):
    conn = get_connection()
    if conn is None:
        print("Fail")
        return
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO CustomizedModel (bmid, mid) VALUES (%s,%s)", (bmid, mid))
        conn.commit()
        print("Success")
    except:
        if conn:
            conn.rollback()
        print("Fail")
    finally:
        if conn:
            conn.close()

# ------------------------------
# Question 4: Delete Base Model
# ------------------------------
def deleteBaseModel(bmid):
    conn = get_connection()
    if conn is None:
        print("Fail")
        return
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT bmid FROM BaseModel WHERE bmid=%s", (bmid,))
        if cursor.fetchone() is None:
            print("Fail")
            return
        cursor.execute("DELETE FROM BaseModel WHERE bmid=%s", (bmid,))
        conn.commit()
        print("Success")
    except:
        if conn:
            conn.rollback()
        print("Fail")
    finally:
        if conn:
            conn.close()

# ------------------------------
# Question 5: List Internet Service
# ------------------------------
def listInternetService(bmid):
    conn = get_connection()
    if conn is None:
        print("Fail")
        return
    try:
        cursor = conn.cursor()
        query = """
            SELECT i.sid, i.endpoint, i.provider
            FROM ModelServices ms
            JOIN InternetService i ON ms.sid=i.sid
            WHERE ms.bmid=%s
            ORDER BY i.provider ASC
        """
        cursor.execute(query, (bmid,))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
            return
        for r in rows:
            print(f"{r[0]},{r[1]},{r[2]}")
    except:
        print("Fail")
    finally:
        if conn:
            conn.close()

# ------------------------------
# Question 6: Count Customized Model
# ------------------------------
def countCustomizedModel(*bmid_list):
    # Handle the case where bmid_list might be a single list
    if len(bmid_list) == 1 and isinstance(bmid_list[0], list):
        bmid_list = bmid_list[0]
    
    conn = get_connection()
    if conn is None:
        print("Fail")
        return
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
            return
        for r in rows:
            print(f"{r[0]},{r[1]},{r[2]}")
    except:
        print("Fail")
    finally:
        if conn:
            conn.close()

# ------------------------------
# Question 7: Top-N Duration Config
# ------------------------------
def topNDurationConfig(uid, N):
    conn = get_connection()
    if conn is None:
        print("Fail")
        return
    try:
        cursor = conn.cursor()
        query = """
            SELECT c.client_uid, mc.cid, c.labels, c.content, mc.duration
            FROM ModelConfigurations mc
            JOIN Configuration c ON mc.cid=c.cid
            WHERE c.client_uid=%s
            ORDER BY mc.duration DESC
            LIMIT %s
        """
        cursor.execute(query,(uid,N))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
            return
        for r in rows:
            print(f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]}")
    except:
        print("Fail")
    finally:
        if conn:
            conn.close()

# ------------------------------
# Question 8: List Base Model Keyword
# ------------------------------
def listBaseModelKeyWord(keyword):
    conn = get_connection()
    if conn is None:
        print("Fail")
        return
    try:
        cursor = conn.cursor()
        query = """
            SELECT DISTINCT b.bmid, l.sid, i.provider, l.domain
            FROM BaseModel b
            JOIN ModelServices ms ON b.bmid=ms.bmid
            JOIN LLMService l ON ms.sid=l.sid
            JOIN InternetService i ON l.sid=i.sid
            WHERE l.domain LIKE %s
            ORDER BY b.bmid ASC
            LIMIT 5
        """
        cursor.execute(query,(f"%{keyword}%",))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
            return
        for r in rows:
            print(f"{r[0]},{r[1]},{r[2]},{r[3]}")
    except:
        print("Fail")
    finally:
        if conn:
            conn.close()

# ------------------------------
# Main entry point
# ------------------------------
if __name__=="__main__":
    if len(sys.argv)<2:
        print("Fail")
        sys.exit(1)

    func = sys.argv[1]
    args = sys.argv[2:]
    args = [None if x=="NULL" else x for x in args]
    try:
        if func=="import":
            import_data(args[0])
        elif func=="insertAgentClient":
            # Convert arguments to appropriate types
            insertAgentClient(int(args[0]), args[1], args[2], int(args[3]),
                              args[4], args[5], int(args[6]), int(args[7]), args[8])
        elif func=="addCustomizedModel":
            addCustomizedModel(int(args[0]), int(args[1]))
        elif func=="deleteBaseModel":
            deleteBaseModel(int(args[0]))
        elif func=="listInternetService":
            listInternetService(int(args[0]))
        elif func=="countCustomizedModel":
            # Handle both single value and list format
            if len(args) == 1 and args[0].startswith("[") and args[0].endswith("]"):
                # Handle list format like "[40,30,45]"
                bmids_str = args[0][1:-1]  # Remove brackets
                if bmids_str.strip() == "":
                    bmids = []
                else:
                    bmids = [int(x.strip()) for x in bmids_str.split(",")]
            else:
                bmids = [int(x) for x in args]
            countCustomizedModel(*bmids)
        elif func=="topNDurationConfig":
            topNDurationConfig(int(args[0]), int(args[1]))
        elif func=="listBaseModelKeyWord":
            listBaseModelKeyWord(args[0])
        else:
            print("Fail")
    except Exception as e:
        print("Fail")
