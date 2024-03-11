
import data_scraper
import menu_collector
import db_handler
import globals
import extract

import logging
import datetime
import pandas as pd
import os

logging.basicConfig(filename=f'/home/sarvamuser/law_sarvam/nbs/SEBI-data-crawler-csv/databases/log_notification_log_05032024.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def check_for_notifs(scraper_obj):

    whats_new_url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingAll=yes"
    database = globals.DB_NAME
    db_handler_obj = db_handler.DBHandler(database)

    past_hash_val = ""
    past_scanning_date = ""
    past_scanning_time = ""
    current_hash_val = scraper_obj.create_hash(whats_new_url)

    logging.info(f"Current hash val, {current_hash_val}")
    
    most_recent_state = db_handler_obj.get_most_recent_hash()
    if most_recent_state:
        print(most_recent_state)
        _id,scanning_date, scanning_time, hashed_val = most_recent_state
        print(f"Most recent row fetched with past_hash_val {_id}, {hashed_val}")
        logging.info(f"Most recent row fetched with past_hash_val : {hashed_val}")
        past_hash_val = hashed_val
    else:
        logging.info(f'Cant fetch recent. No rows found in the database.')
        print("No rows found in the database.")
    
    print(f"hashes : current_hash_val : {current_hash_val} AND past_hash_val:{past_hash_val}")
    
    if(past_hash_val != "" and current_hash_val != past_hash_val):
        print("hashes are not equal.")
        logging.info(f'Hashes of current and past are not equal')
        most_recent_notifs = db_handler_obj.get_most_recent_notifs()
        
        new_notifs = scraper_obj.find_new_notifs(most_recent_notifs)
        
        print(f"No of new notifs : {len(new_notifs)}")
        logging.info(f'No of new notifs : {len(new_notifs)}')
        print("New Notifications : ")
        logging.info(f'Adding new notifications...')
        logging.info(new_notifs)
        for row in new_notifs:
            logging.info(f'New Notifs detected => Row : {row}')
            print(row)

        scraper_obj.save_to_db(new_notifs)
        scraper_obj.save_current_state()

        scraper_obj.store_new_notifs_to_kb(new_notifs)
    else:
        print("hashes are equal")
        logging.info("Hashes are equal => no new notifs")
        
        
    
if __name__ == '__main__':
    scraper = data_scraper.SEBIDataScraper()
    check_for_notifs(scraper)