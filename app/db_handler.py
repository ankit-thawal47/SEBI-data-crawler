import sqlite3
import os
from datetime import datetime
import data_scraper
import globals
import pytz


import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DBHandler:
    # database = config.DATABASE_NAME
    # table_name = config.TABLE_NOTIFICATIONS
    
    def __init__(self, db_name):
        self.db_name = db_name
        self.db_table_notif_data = globals.TABLE_NOTIF_DATA
        self.db_table_state_storer = globals.TABLE_STATE_STORE_OF_WHATSNEW
        
        if not os.path.exists(db_name):
            print("Database does not exist. Creating it....")
            open(db_name, 'w').close()
            
            # As the db is created fresh, populate it with all the notifications of now
            
            self.create_tables()
            
            data_scarper_obj = data_scraper.SEBIDataScraper()
            data_scarper_obj.download_all_notif_links()
            data_scarper_obj.save_initial_state()
            
            
        else:
            print(f"Database {self.db_name} already exists")
            # data_scarper_obj = data_scraper.DataScraper()
            # data_scarper_obj.download_all_notif_links()
        print("database handler obj is created")

    def create_tables(self):
        # Connect to the database
        database_name = self.db_name
        db_table_notif_data = self.db_table_notif_data
        print(f"Creating table [{db_table_notif_data}] in database :[{database_name}]")
        
        try:
            database_name = self.db_name
            db_table_notif_data = self.db_table_notif_data
            
            db_table_state_storer = self.db_table_state_storer
            
            conn = sqlite3.connect(database_name)
            cursor = conn.cursor()
            
            # Query to create a db and table
            # query = f'''CREATE TABLE IF NOT EXISTS {db_table_notif_data} (
            #                 _id INTEGER PRIMARY KEY AUTOINCREMENT,
            #                 scanning_date TEXT,
            #                 scanning_time TEXT,
            #                 url TEXT,
            #                 hash_hex TEXT,
            #                 html_page TEXT
            #                 )'''
            
            query1 = f'''CREATE TABLE IF NOT EXISTS {db_table_notif_data} (
                            url TEXT PRIMARY KEY,
                            notif_date TEXT,
                            notif_category TEXT,
                            notif_title TEXT,
                            notif_metadata TEXT
                        )'''
            
            query2 = f'''CREATE TABLE IF NOT EXISTS {db_table_state_storer} (
                        _id INTEGER PRIMARY KEY AUTOINCREMENT,
                        scanning_date TEXT,
                        scanning_time TEXT,
                        hashed_val TEXT 
                        )'''

            cursor.execute(query2)
            cursor.execute(query1)
            
            conn.commit()
            print("tables successfully created!")
        except Exception as e:
            print("Exception occured while creating table : ",e)
        finally:
            if conn:
                conn.close()
            
    
    # Past = def fetch_all(self):
    def fetch_all(self):
        db_table_notif_data = self.db_table_notif_data
        query = f"SELECT * FROM {db_table_notif_data}"
        return self.execute_query(query)

    def save_current_state(self, url, hash_hex):
        # Connect to the database
        database_name = self.db_name
        print(f"Inserting values into {self.db_name} and table {self.db_table_state_storer}")
        
        db_table_state_storer = self.db_table_state_storer
        ist = pytz.timezone('Asia/Kolkata')
        #  = ""conn
        try:
            conn = sqlite3.connect(database_name)
            cursor = conn.cursor()

            # Get current date and time
            current_date = datetime.now().date().strftime('%Y-%m-%d')
            current_time = datetime.now(ist).time().strftime('%H:%M:%S')

            # Insert data into the database
            cursor.execute(f'''INSERT INTO {db_table_state_storer} (scanning_date, scanning_time, hashed_val)
                            VALUES (?, ?, ?)''', (current_date, current_time, hash_hex))

            # Commit changes and close connection
            conn.commit()
            print(f"Current state saved successfully in {db_table_state_storer}")
        except Exception as e:
            print(e)
        finally:
            if conn:
                conn.close()

            
    def get_most_recent_notifs(self):

        table_notif_data = self.db_table_notif_data

        most_recent_notifs = []
        try:
            query = f"SELECT * FROM {table_notif_data} WHERE notif_date = (SELECT MAX(notif_date) FROM {table_notif_data});"
            # query = f"SELECT * FROM {table_notif_data} ORDER BY notif_date DESC;"
            most_recent_notifs = self.execute_query(query)         
        except Exception as e:
            print("Exception occurred : ",e)


        return most_recent_notifs

    def store_notifications_data(self, database_name, table_name, list_of_data):
        if(len(list_of_data) == 0):
            print("data list is empty")
        try:
            # Connect to the SQLite database (or create it if it doesn't exist)
            conn = sqlite3.connect(database_name)
            c = conn.cursor()

            # query = f'''CREATE TABLE IF NOT EXISTS {table_name}
            #             (name text, age integer)'''
            

            # c.execute(query)
            for row in list_of_data:
                print(row)
    
            # c.executemany(f'INSERT INTO {table_name} VALUES (?, ?, ?, ?)', list_of_data)
            conn.commit()
        except Exception as e:
            print("Error occured : ",e)
        finally:
            if conn:
                conn.close()

    
    def execute_query(self, query):
        results = []
        try:
            database_name = self.db_name
            conn = sqlite3.connect(database_name)
            cursor = conn.cursor()
            
            cursor.execute(query)
            results = cursor.fetchall()
        except Exception as e:
            print(e)
        finally:
            if conn:
                conn.close()
                
        return results
    
    def get_most_recent_hash(self):
        database_name = self.db_name
        table_state_storer = self.db_table_state_storer
        # conn = ""
        most_recent_row = []
        try:
            conn = sqlite3.connect(database_name)
            cursor = conn.cursor()
            # Execute the query to get the most recent row
            cursor.execute(f"SELECT * FROM {table_state_storer} ORDER BY scanning_date DESC, scanning_time DESC LIMIT 1;")
            most_recent_row = cursor.fetchone()           
        except Exception as e:
            print("Exception occurred : ",e)
        finally:
            if conn:
                conn.close()

        return most_recent_row
    
    def delete_when_the_url_is_null(self):
        
        print("inside delete_when_the_url_is_null")
        import sqlite3

        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Define the DELETE statement
        delete_query = f"DELETE FROM {self.db_table_notif_data} WHERE url IS NULL OR url = ''"

        # delete_query = f"DELETE FROM {self.db_table_notif_data} WHERE url = ''"

        try:
            # Execute the DELETE statement
            cursor.execute(delete_query)
            # Commit the transaction
            conn.commit()
            print("Rows with empty URL deleted successfully.")
        except sqlite3.Error as e:
            print("Error deleting rows:", e)
        finally:
            # Close the cursor and connection
            cursor.close()
            conn.close()

    
    def change_date_formats(self):
        import sqlite3
        from datetime import datetime
        
        print("inside date change format")

        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Retrieve the dates from the database
        cursor.execute("SELECT url,notif_date FROM notif_data")
        rows = cursor.fetchall()

        # Iterate over the rows and convert the dates
        for row in rows:
            # Parse the date string into a datetime object
            original_date_str = row[1]  # Assuming the date is in the second column
            original_date = datetime.strptime(original_date_str, '%b %d, %Y')

            # Convert the date to a sortable format (e.g., ISO-8601)
            sortable_date_str = original_date.strftime('%Y-%m-%d')
            
            print(f"changed date from {original_date} to {sortable_date_str} for url = {row[0]}")

            # Update the database with the converted date
            cursor.execute("UPDATE notif_data SET notif_date = ? WHERE url = ?", (sortable_date_str, row[0]))  # Assuming 'id' is the primary key column

        # Commit the changes to the database
        conn.commit()

        # Close the connection
        conn.close()

    def print_rows_using_date(self,date):
        print("inside print_rows_using_date")
        import sqlite3

        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Define the DELETE statement
        
        delete_query = f"DELETE FROM {self.db_table_state_storer} WHERE scanning_date = '26-02-2024'"

        # delete_query = f"DELETE FROM {self.db_table_notif_data} WHERE url = ''"

        try:
            # Execute the DELETE statement
            cursor.execute(delete_query)
            # Commit the transaction
            conn.commit()
            print("Rows with date deleted successfully.")
        except sqlite3.Error as e:
            print("Error deleting rows:", e)
        finally:
            # Close the cursor and connection
            cursor.close()
            conn.close()
