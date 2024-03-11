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

        ##########################################
    #     df = pd.read_csv(globals.pdf_links_of_all)

    #     for index,row in df.iterrows():
    #         print(index)
    #         pdf_link = row['pdf_link']
    #         html_url = row['html_link']
    #         title = row['title']
    #         type = row['type']
    #         sub_type = row['sub_type']
    #         file_name = row['file_name']
    #         # print(df.loc(index,'file_name'))
            
    #         if(index > 10):
    #             break

    #         print(type,sub_type,file_name)
            
    #         if(pd.isna(type) or pd.isna(sub_type) or pd.isna(file_name)):
    #             continue
            
    #         def create_download_path(type, sub_type):
    #             type = type.replace(" ","_")
    #             sub_type = sub_type.replace(" ","_")
                
    #             type_folder_path = os.path.join(globals.SEBI_data_extraction_base_folder,type)
    #             sub_type_folder_path = os.path.join(type_folder_path,sub_type)
    #             download_path = sub_type_folder_path
    #             return download_path
                
    #         download_path = create_download_path(type,sub_type)
    #         file_path = os.path.join(download_path,file_name)
            
    #         if(not os.path.exists(file_path)):
    #             continue
            
    #         text = ""
    #         # if(file_name.endswith("pdf")):
    #         #     text = extract.extract_data_from_pdf(file_path)
    #         # elif(file_name.endswith("html")):
    #         #     text = extract.extract_data_from_html(file_path)
    #         print(text)
    # except Exception as e:
    #     print(e)
            
def main():
    menu_collectors_obj = menu_collector.SEBIMenuCollector()
    scraper = data_scraper.SEBIDataScraper()
    menu_collectors_obj.collect_menu_links()
    menu_collectors_obj.create_folder_hierarchy()
        
    # check_for_notifs(scraper)
    extract.extract_data()
    
    # list_of_menus = ["about", "legal", "enforcement", "filings", "reports & statistics", "status", "media & notifications"]
    # scraper.collect_pdf_links("legal", "acts")
    # scraper.collect_pdf_links("legal", "rules")
    # scraper.collect_pdf_links("legal", "regulations")
    # scraper.collect_pdf_links("legal", "general orders")
    # scraper.collect_pdf_links("legal", "guidelines")
    # scraper.collect_pdf_links("legal", "master circulars")
    # scraper.collect_pdf_links("legal", "circulars")
    # scraper.collect_pdf_links("legal", "gazette notification")
    # scraper.collect_pdf_links("enforcement", "orders")
    # scraper.collect_pdf_links("enforcement", "informal_guidance")
    
    # Downloading
    # scraper.download_files("legal","acts")
    
    # scraper.download_files3("legal","regulations")
    # scraper.download_files3("legal","rules")
    # print("Download Started!!")
    # scraper.download_files3("legal","acts")
    # scraper.download_files3("legal","circulars")
    # scraper.download_pdf("https://www.sebi.gov.in/web/?file=https://www.sebi.gov.in/sebi_data/attachdocs/mar-2023/1679919997585.pdf", r"/home/sarvamuser/law_sarvam/nbs/SEBI_Ankit/SEBI_Extracted_Data/enforcement/orders", "1679919997585.pdf")
    # scraper.download_files3("enforcement","orders")
    # scraper.download_files3("enforcement", "informal_guidance")
    
    
    print(globals.SEBI_data_extraction_base_folder)
    
    
    
if __name__ == '__main__':
    main()
    
