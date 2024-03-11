import requests
from bs4 import BeautifulSoup
import io
import datetime
import csv
import pandas as pd
from urllib.parse import urljoin
import re
import hashlib
import os
import base64
import pandas as pd
import urllib3
import fitz
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
from pprint import pprint
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
import concurrent.futures
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import globals
import sqlite3
from datetime import datetime

from bs4 import BeautifulSoup

import db_handler

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



class SEBIDataScraper:

    def __init__(self):
        print("SEBI Data Scrapper Object is created")
        # Create CSV files with menus, if they dont already exists
        if(not os.path.exists(globals.pdf_links_of_all)):
            df = pd.DataFrame(columns=globals.columns_for_pdf_links_of_all)
            df.to_csv(globals.pdf_links_of_all, index=True)
            print(f"CSV file {globals.pdf_links_of_all} with column names created successfully.")
        self.data = []
        
        self.db_handler_obj = db_handler.DBHandler(globals.DB_NAME)

    def create_folder_hierarchy(self):
        try:
            df = pd.read_csv(globals.urls_of_sebi_menu_csv_path)

            # Base folder to store SEBI Extracted Data
            base_folder_path = os.path.join(globals.regChat_folder,"SEBI_Extracted_Data")
            print(base_folder_path)
            # base_folder_path = base_folder_path.lower()
            if not os.path.exists(base_folder_path):
                # Create the base folder if it doesn't exist
                os.makedirs(base_folder_path)

            for _,row in df.iterrows():
                menu = row['menu']
                menu = menu.replace(" ","_")
                menu = menu.lower()
                print(menu)
                menu_folder = os.path.join(base_folder_path,menu)
                # Create the base folder if it doesn't exist
                if not os.path.exists(menu_folder):
                    os.makedirs(menu_folder)
                sub_menu = row['submenu']
                sub_menu = sub_menu.lower()
                sub_menu = sub_menu.replace(" ","_")
                sub_menu_folder = os.path.join(menu_folder,sub_menu)
                if not os.path.exists(sub_menu_folder):
                    os.makedirs(sub_menu_folder)
        except Exception as e:
            print(f"Exception occurred : {e}")
        finally:
            print("Folder Hierarchy Created")

    def navigate_pagination_and_collect_links(self,url,type,sub_type):
        type = type.lower()
        sub_type = sub_type.lower()
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        page_num = 0

        while(1):
            print("inside while")
            logging.basicConfig(filename='selenium_next_button.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
            logging.info(f'Entering navigate_pagination_and_collect_links method, {url}')

            try:

                time.sleep(1)
                ########### GEL HTML/PDF LINKS FROM THE TABLE ###########
                html_content = driver.page_source
                soup = BeautifulSoup(html_content,'html.parser')
                table = soup.find('table')
                if(table == None):
                    return
                no_of_rows = len(table.find_all('tr'))
                print(f"Page : {page_num} | No of rows: {no_of_rows}")

                # The html contains table which hold info - PDF Title, Data, PDF Viewer Link
                for row in table.find_all('tr'):
                    if(row == None):
                        continue
                    all_cells = row.find_all('td')
                    #Exclude the first row
                    if(len(all_cells) == 0):
                        continue

                    anchor_tag = all_cells[1].find('a')
                    date = all_cells[0].text
                    title = all_cells[1].text
                    href_link = anchor_tag.get('href')
                    new_row = {
                        "title" : title,
                        "date" : date,
                        "html_link" : href_link,
                        "pdf_link" : "",
                        "type": type,
                        "sub_type" : sub_type,
                        "file_name" : "",
                        "pdf_text" : ""
                    }

                    self.data.append(new_row)

                ############ ENDS HERE ############

                time.sleep(2)

                # Check if the page contains pagination or is it a single page
                if not (driver.find_element(By.CLASS_NAME, "pagination_outer")):
                    return

                WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "pagination_outer"))
                    # Check if the pagination_outer is loaded, so we can move on to next step
                )

                # if we cant find the next_button, it means we are at the last page
                if not (driver.find_elements(By.XPATH, "//*[@title='Next']")):
                    return

                next_button = driver.find_element(By.XPATH, "//*[@title='Next']")
                time.sleep(2)

                next_button.click()
                logging.info('Clicked on Next Button')

                driver.implicitly_wait(5)
                logging.info('Waited for download to complete')
            except Exception as e:
                print("EXCEPTION occurred :",e)
                logging.error(f'An exception occurred: {str(e)}')
                return
            finally:
                #driver.quit()
                page_num += 1
                print("Exiting navigation_pagination_and_collect_links")
                logging.info('Exiting navigation_pagination_and_collect_links')

    # Function that collects html links from navigating to each menu links
    def collect_html_links(self,menu_to_scrape, submenu_to_scrape):
        print(f"Entering collect_html_links for [{menu_to_scrape}] | [{submenu_to_scrape}]")
        df = pd.read_csv(globals.urls_of_sebi_menu_csv_path)
        
        for _,row in df.iterrows():
            menu = row['menu']
            submenu = row['submenu']
            url = row['url']
            # print(f"Creating list of urls using menu: {menu} and sub_menu: {submenu}")
            menu = menu.replace(" ","_")
            submenu = submenu.replace(" ","_")
            if(menu.lower() == menu_to_scrape.lower() and submenu.lower() == submenu_to_scrape.lower()):
                print(f"Accessing the link {url}")
                self.navigate_pagination_and_collect_links(url,menu,submenu)
            # print(f"Row added in CSV: {menu} | sub_menu: {submenu}")
    
    def collect_pdf_links(self,menu_to_scrape, submenu_to_scrape):

        print(f"collecting pdf links for : [{menu_to_scrape}] | [{submenu_to_scrape}]")

        for row in self.data:
            url = row['html_link']
            soup = self.soup_returner(url)
            only_anchor_tags = soup.find_all('iframe')
            new_pdf_link = ""
            for link in only_anchor_tags:
                href_link = link.get('src')
                if href_link!=None and href_link.lower().endswith(".pdf"):
                    pdf_link = href_link
                    new_pdf_link = urljoin(globals.home_page_url,pdf_link)
            row['pdf_link'] = new_pdf_link

            #storing the name of the pdf for the future use
            split_pdf_link = new_pdf_link.split("/")
            row['file_name'] = split_pdf_link[-1]
            
            # If thge pdf name is blank that means the content is html file
            if (new_pdf_link == ""):
                url_base64 = base64.b64encode(url.encode()).decode()
                row['file_name'] = url_base64+".html"

        df2 = pd.DataFrame(self.data)
        df2.to_csv(globals.pdf_links_of_all, mode='a', index=False)

    # @staticmethod
    def download_pdf(self,url,download_path,file_name):
        logging.basicConfig(filename='selenium_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
        logging.info(f"Downloading {file_name} in {download_path}")
        print(f"Downloading {file_name} in {download_path}")

        if(not url.startswith("http")):
            logging.info("Not a valid url")
            return
        
        pdf_path = os.path.join(download_path,file_name)
        if(os.path.exists(pdf_path)):
            print(f"The pdf {file_name} already exists")
            logging.info(f"The pdf {file_name} already exists")
            return

        try:
            response = requests.get(url)
            logging.info(f'Status of url : {url} is {response.status_code}')
            if(response.status_code != 200):
                print(f"Status of {url} is {response.status_code}")
            options = webdriver.ChromeOptions()

            prefs = {
                "download.default_directory": download_path,
                'download.prompt_for_download': False,
                'plugins.always_open_pdf_externally': True
            }

            options.add_argument("--headless=new")

            options.add_experimental_option('prefs',prefs)

            driver = webdriver.Chrome(options=options)
            driver.get(url)
            logging.info(f'Navigated to URL: {url}')
            # Wait for some time to ensure the PDF is loaded
            #driver.implicitly_wait(10)

            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, "download"))
            )

            #Find the DOWNLOAD Button from the pdfviewer using the id of button tag
            download_button = driver.find_element(By.ID, "download")
            time.sleep(3)
            download_button.click()
            logging.info('Clicked on PDF download Button')
            driver.implicitly_wait(10)
            logging.info('Waited for download to complete')
            time.sleep(6)

        except Exception as e:
            print("EXCEPTION ",e)
            logging.error(f'An error occurred: {str(e)}')
        finally:
            #driver.quit()
            logging.info('Browser session closed')

    def download_pdf_new(self,row):
        
        print("ROW RECEIVED :")
        print(row)
        
        url = row['pdf_link']
        html_url = row['html_link']
        title = row['title']
        type = row['type']
        sub_type = row['sub_type']
        file_name = row['file_name']
        
        type = type.replace(" ","_")
        sub_type = sub_type.replace(" ","_")
        # SEBI_data_extraction_base_folder = r"D:\Educational\Sarvam AI\Python\SEBI\SEBI_Extracted_Data"
        type_folder_path = os.path.join(globals.SEBI_data_extraction_base_folder,type)
        sub_type_folder_path = os.path.join(type_folder_path,sub_type)
        download_path = sub_type_folder_path
        
        print("URL Accessing ",url)
        
        file_path = os.path.join(download_path, file_name)
        if(os.path.exists(file_path)):
            print(f"the file {file_path} already exists.")
            return
        
        try:
            response = requests.get(url)
            # logging.info(f'Status of url : {url} is {response.status_code}')
            if(response.status_code != 200):
                print(f"Status of {url} is {response.status_code}")
            options = webdriver.ChromeOptions()

            prefs = {
                "download.default_directory": download_path,
                'download.prompt_for_download': False,
                'plugins.always_open_pdf_externally': True
            }

            options.add_argument("--headless=new")

            options.add_experimental_option('prefs',prefs)

            driver = webdriver.Chrome(options=options)
            driver.get(url)

            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, "download"))
            )

            #Find the DOWNLOAD Button from the pdfviewer using the id of button tag
            download_button = driver.find_element(By.ID, "download")
            time.sleep(2)
            download_button.click()
            
            driver.implicitly_wait(10)
            # logging.info('Waited for download to complete')
            time.sleep(6)
            
            timeout = 25  # Maximum wait time in seconds
            start_time = time.time()
            while time.time() - start_time < timeout:
                print(str(time.time() - start_time))
                if os.path.isfile(os.path.join(download_path, file_name)):
                    print(f"Download complete! for {file_name} in {download_path}")
                    break
                time.sleep(1)  # Check every 1 second
        except NoSuchElementException:
            print(f"Download button not found for URL: {url}")
        finally:
            # driver.quit()
            print("Exiting download_pdf_new")
    
    # @staticmethod
    def download_html(self,url,download_path, file_name):
        print(f"Downloading {file_name} in {download_path}")
        response = requests.get(url)
        
        # filename_hashed = hashlib.sha256(url.encode('utf-8')).hexdigest()

        file_download_path = os.path.join(download_path,file_name)
        if(os.path.exists(file_download_path)):
            print(f"The file {file_name} already exists")
            logging.info(f"The file {file_name} already exists")
            return

        if response.status_code == 200:
            print(response.status_code)
            with open(file_download_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"{url} is downloaded into {download_path}")
        else:
            print("Failed to download HTML:", response.status_code)
            
    def download_html_new(self,row):
        # url = row['pdf_link']
        html_url = row['html_link']
        title = row['title']
        type = row['type']
        sub_type = row['sub_type']
        file_name = row['file_name']

        print(f" File : {file_name} and LENGTH : {len(file_name)}",file_name)

        type = type.replace(" ","_")
        sub_type = sub_type.replace(" ","_")
        type_folder_path = os.path.join(globals.SEBI_data_extraction_base_folder,type)
        sub_type_folder_path = os.path.join(type_folder_path,sub_type)
        download_path = sub_type_folder_path
        
        file_path = os.path.join(download_path, file_name)
        if(os.path.exists(file_path)):
            print(f"the file {file_path} already exists.")
            return
        
        response = requests.get(html_url)

        if response.status_code == 200:
            print(response.status_code)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"{html_url} is downloaded into {download_path}")
        else:
            print("Failed to download HTML:", response.status_code)
    
    
    def count_files(self, menu,submenu,file_type):
    # Ensure the folder exists

        folder_path = os.path.join(globals.SEBI_data_extraction_base_folder,menu)
        folder_path = os.path.join(folder_path,submenu)
        if not os.path.exists(folder_path):
            print(f"The folder '{folder_path}' does not exist.")
            return

        # Initialize the count
        file_count = 0
        total_files = 0
        # Iterate through all items in the folder
        for item in os.listdir(folder_path):
            # Check if the item is a file
            if os.path.isfile(os.path.join(folder_path, item)):
                # If it's a file, increment the count
                total_files += 1
                if(item.lower().endswith(file_type)):
                    file_count += 1

        return file_count,total_files
    
    def create_list_of_links(self,menu,submenu):

        df = pd.read_csv(globals.pdf_links_of_all)
        pdf_link_list = []
        html_link_list = []
        total_pdf_count = 0
        total_html_count = 0
        
        print(f"Create list of links {menu} and {submenu}")
        
        i = 0
        for _,row in df.iterrows():
            
            pdf_link = row['pdf_link']
            html_url = row['html_link']
            title = row['title']
            type = row['type']
            sub_type = row['sub_type']
            file_name = row['file_name']
            flag = False
            
            if(pd.isna(row['pdf_link'])):
                flag = True
            if(pd.isna(html_url) or pd.isna(title) or pd.isna(type) or pd.isna(sub_type) or pd.isna(file_name)):
                # print("Values are empty")
                continue
            
            def create_download_path(type, sub_type):
                type = type.replace(" ","_")
                sub_type = sub_type.replace(" ","_")
                
                type_folder_path = os.path.join(globals.SEBI_data_extraction_base_folder,type)
                sub_type_folder_path = os.path.join(type_folder_path,sub_type)
                download_path = sub_type_folder_path
                return download_path
            
            download_path = create_download_path(type,sub_type)
            file_path = os.path.join(download_path,file_name)
            
            if(not pd.isna(pdf_link)):
                if(menu == str(row['type']) and submenu == str(row['sub_type'])):
                    # print("pdf_list udpated")
                    total_pdf_count += 1
            else:
                if(menu == str(row['type']) and submenu == str(row['sub_type'])):
                    # print("html list updated")
                    total_html_count += 1
            
            
            # print(f"TYPE : {type} SUBTYPE : {sub_type}")
            if(not pd.isna(pdf_link)):
                if(menu == str(row['type']) and submenu == str(row['sub_type'])):
                    # print("pdf_list udpated")
                    if(os.path.exists(file_path)):
                        print(f"Not adding to list, becoz The file {file_path} already exists")
                        continue
                    pdf_link_list.append(row)
            else:
                if(menu == str(row['type']) and submenu == str(row['sub_type'])):
                    # print("html list updated")
                    if(os.path.exists(file_path)):
                        print(f"Not adding to list, becoz The file {file_path} already exists")
                        continue
                    html_link_list.append(row)
                    
            # DELETE IT LATER
            if(len(pdf_link_list) == 4000):
                return pdf_link_list,html_link_list,total_pdf_count,total_html_count
                
        return pdf_link_list,html_link_list,total_pdf_count,total_html_count
    
    def download_files(self, menu, sub_menu):
        df = pd.read_csv(globals.pdf_links_of_all)
        
        for _,row in df.iterrows():
            pdf_url = row['pdf_link']
            html_url = row['html_link']
            title = row['title']
            type = row['type']
            sub_type = row['sub_type']
            file_name = row['file_name']
            flag = False
            if(pd.isna(row['pdf_link'])):
                flag = True
            if(pd.isna(html_url) or pd.isna(title) or pd.isna(type) or pd.isna(sub_type) or pd.isna(file_name)):
                print("Values are empty.")
                continue
            
            type = type.replace(" ","_")
            sub_type = sub_type.replace(" ","_")
            
            type_folder_path = os.path.join(globals.SEBI_data_extraction_base_folder,type)
            sub_type_folder_path = os.path.join(type_folder_path,sub_type)
            download_path = sub_type_folder_path
            
            if (sub_menu == None):
                if(menu == type.lower()):
                    if (flag):
                        file_name = row['file_name']
                        filename_hashed = hashlib.sha256(html_url.encode('utf-8')).hexdigest()
                        row['file_name'] = filename_hashed
                        self.download_html(html_url, download_path, filename_hashed)
                    else:
                        self.download_pdf(pdf_url, download_path,file_name)
            else:
                if(menu == type.lower() and sub_menu == sub_type.lower()):
                    if (flag):
                        file_name = row['file_name']
                        filename_hashed = hashlib.sha256(html_url.encode('utf-8')).hexdigest()
                        row['file_name'] = filename_hashed
                        self.download_html(html_url, download_path, filename_hashed)
                    else:
                        self.download_pdf(pdf_url, download_path,file_name)
                        
    def download_files2(self, row, menu, sub_menu):
        
        pdf_url = row['pdf_link']
        html_url = row['html_link']
        title = row['title']
        type = row['type']
        sub_type = row['sub_type']
        file_name = row['file_name']
        
        print(f"Downloading..... {html_url}")
        
        type = type.replace(" ","_")
        sub_type = sub_type.replace(" ","_")
        
        type_folder_path = os.path.join(globals.SEBI_data_extraction_base_folder, type)
        sub_type_folder_path = os.path.join(type_folder_path, sub_type)
        download_path = sub_type_folder_path

        if not os.path.exists(download_path):
            os.makedirs(download_path)
            
        if (sub_menu == None):
            if(menu == type.lower()):
                if (pdf_url == ""):
                    self.download_html(html_url, download_path, file_name)
                else:
                    self.download_pdf(pdf_url, download_path,file_name)
            else:
                if(menu == type.lower() and sub_menu == sub_type.lower()):
                    if (pdf_url == ""):
                        self.download_html(html_url, download_path, file_name)
                    else:
                        self.download_pdf(pdf_url, download_path,file_name)
                        
    def soup_returner(self,url):
        soup = BeautifulSoup()
        try:
            session = requests.Session()
            retry = HTTPAdapter(max_retries=5)
            session.mount("http://", retry)
            session.mount("https://", retry)
            read = session.get(url,verify=False)
            html_content = read.text
            soup = BeautifulSoup(html_content,'html.parser')
        except Exception as e:
            print("URL ", url)
            print("Exception occured : ",e )
        return soup
                        
    def download_files3(self,menu,submenu):
        print("inside download_files3")
        print(menu,submenu)
        pdf_urls = []
        html_urls = []
        pdf_urls,html_urls,total_pdf_count,total_html_count = self.create_list_of_links(menu,submenu)
        for row in pdf_urls:
            print(row)
            print(row['pdf_link'])
            print("-------------------")
        print("HTML Links ::: ")
        for row in html_urls:
            print(row['html_link'])
            print("-------------------") 
        
        print("List has been created, moving on to downloads")
        
        # Concurrently download PDFs
        round = 1
        # file_count,total_files_downloaded = self.count_files(menu,submenu,"pdf")
        i = 0

        while(1):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(self.download_pdf_new, pdf_urls)
            pdf_urls = []
            html_urls = []
            pdf_urls,html_urls,total_pdf_count,total_html_count = self.create_list_of_links(menu,submenu)
            
        round = 1
        i = 0
        file_count,total_files_downloaded = self.count_files(menu,submenu,"html")
        for row in html_urls:
            self.download_html_new(row)
            print(f"Download completed : {i}/{len(html_urls)} ")
            i += 1

    def find_table(self,whats_new_url):
        soup = self.soup_returner(whats_new_url)
        table = soup.find('table', id='sample_1')
        return str(table)
    
    def create_hash(self,whats_new_url):
        soup = self.soup_returner(whats_new_url) 
        table = soup.find('table', id='sample_1')
        hash_object = hashlib.sha256()
        hash_object.update(table.encode('utf-8'))
        hash_hex = hash_object.hexdigest()
        # return hash_hex,str(table)
        return hash_hex
    
    def create_list_of_rows_from_table_html(self,html_content_of_table):
        list_of_rows = []
        soup = BeautifulSoup(html_content_of_table, 'html.parser')
        rows = soup.find_all('tr')

        # Iterate over each row
        for row in rows:
            # Extract data from each column
            temp = []
            columns = row.find_all('td')
            date = columns[0].text
            category = columns[1].text
            
            link = columns[2].find('a')
            title = link.text
            url = link['href']
            
            date = utils.date_formatter(date)
            
            temp = {
                "date" : date,
                "category" : category,
                "url" : url,
                "title" : title,
            }
            list_of_rows.append(temp)
        return list_of_rows
    
    def get_pdf_url(self,html_url):
        # url = row['html_link']
        soup = self.soup_returner(html_url)
        BASE_URL = "https://www.sebi.gov.in/"
        iframe_tag = soup.find_all('iframe')
        pdf_link = ""
        
        
        if iframe_tag:
            for link in iframe_tag:
                pdf_link = link.get('src')
                if pdf_link and pdf_link.lower().endswith(".pdf"):
                    pdf_link = urljoin(BASE_URL,pdf_link)
        else:
            all_anchor_tags = soup.find_all('a')
            for link in all_anchor_tags:
                pdf_link = link.get('href')
                if pdf_link and pdf_link.lower().endswith(".pdf"):
                    pdf_link = urljoin(BASE_URL,pdf_link)
                    
        return pdf_link
    
    def save_to_db(self,data):
        #
        print("saving data in db.....")
        
        try:
            # Connect to the SQLite database (or create it if it doesn't exist)
            conn = sqlite3.connect(globals.DB_NAME)
            cursor = conn.cursor()
            
            for row in data:
                # print(row)
                date = row['date']
                category = row['category']
                url = row['url']
                title = row['title']
                
                try:
                    print("INSERTING INTO DB : ",date,category,url,title)
                    
                    query = f'''INSERT INTO {globals.TABLE_NOTIF_DATA} (url, notif_date, notif_category, notif_title)
                                    VALUES (?,?,?,?)'''
                    cursor.execute(query,(url,date,category,title))
                except Exception as e:
                    print("FOR ROW", row)
                    print("Exception occured..",e)
                    

            # Commit the changes
            conn.commit()
            print("Data saved successfully to database.")
        except sqlite3.Error as e:
            print("SQLite error:", e)
        finally:
            # Close the database connection
            if conn:
                conn.close()
                
    # def save_to_db(self,data):
    #     #
    #     print("saving data in db.....")
        
    #     try:
    #         # Connect to the SQLite database (or create it if it doesn't exist)
    #         conn = sqlite3.connect(self.db_name)
    #         cursor = conn.cursor()
            

    #         # Insert data from the list into the database table
    #         # cursor.executemany(f'INSERT INTO {db_constant.TABLE_NOTIF_DATA} VALUES (?, ?, ?, ?)', data)
            
    #         for row in data:
    #             # print(row)
    #             date = row['date']
    #             category = row['category']
    #             url = row['url']
    #             title = row['title']
    #             try:
    #                 print("INSERTING INTO DB : ",date,category,url,title)
                    
    #                 #             url TEXT PRIMARY KEY,
    #                 #             notif_date TEXT,
    #                 #             notif_category TEXT,
    #                 #             notif_title TEXT,
    #                 #             notif_metadata TEXT
                    
    #                 query = f'''INSERT INTO {db_constant.TABLE_NOTIF_DATA} (url, notif_date, notif_category, notif_title)
    #                                 VALUES (?,?,?,?)'''
    #                 cursor.execute(query,(url,date,category,title))
    #             # cursor.execute(f'''INSERT INTO {db_table_state_storer} (scanning_date, scanning_time, hashed_val)
    #             #             VALUES (?, ?, ?)''', (current_date, current_time, hash_hex))
    #             except Exception as e:
    #                 print("FOR ROW", row)
    #                 print("Exception occured..",e)
                    

    #         # Commit the changes
    #         conn.commit()
    #         print("Data saved successfully to database.")
    #     except sqlite3.Error as e:
    #         print("SQLite error:", e)
    #     finally:
    #         # Close the database connection
    #         if conn:
    #             conn.close()

    def save_initial_state(self):
        print("Saving initial state.")
        whats_new_url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingAll=yes"
        current_hash_hex = self.create_hash(whats_new_url)
        self.db_handler_obj.save_current_state(whats_new_url,current_hash_hex)
        
    def save_current_state(self):
        print("Saving current state.")
        whats_new_url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingAll=yes"
        current_hash_hex = self.create_hash(whats_new_url)
        self.db_handler_obj.save_current_state(whats_new_url,current_hash_hex)
        
    def format_date_for_sqlite(self,date_str):
        # Parse the input date string
        parsed_date = datetime.strptime(date_str, "%b %d, %Y")
        # Format the parsed date into SQLite sortable format (YYYY-MM-DD)
        sqlite_formatted_date = parsed_date.strftime("%Y-%m-%d")
        return sqlite_formatted_date
    
    def find_new_notifs(self,most_recent_notifs):
        set_of_most_recent_pdf_links = set()
        print("inside find_new_notifs")
        
        for row in most_recent_notifs:
            html_link = row[0]
            set_of_most_recent_pdf_links.add(html_link)
        
        print("set_of_most_recent_html_links :")
        print(set_of_most_recent_pdf_links)
        
        url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingAll=yes"
            
        new_notifs = []
        print("inside find_new_notifs")
        
        options = webdriver.ChromeOptions()
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument("--headless=new")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        
        page_num = 0

        while(1):
            print("inside while")
            try:
                print(f"Page : {page_num}")
                # The html contains table which hold info - PDF Title, Data, PDF Viewer Link
                
                time.sleep(5)
                ########### GEL HTML/PDF LINKS FROM THE TABLE ###########
                html_content = driver.page_source
                soup = BeautifulSoup(html_content,'html.parser')
                
                table = soup.find('table')

                if(table == None):
                    return
                no_of_rows = len(table.find_all('tr'))

                print(f"Page : {page_num} | No of rows: {no_of_rows}")

                all_rows = table.find_all('tr')
                print(f"Number of rows in page:{page_num} :: ",len(all_rows))
                if(len(all_rows) == 0):
                    break
                i = 1
                for row in all_rows:
                    # print(row)
                    print("------------------------")
                    if(row == None):
                        continue
                    all_table_data_cells = row.find_all('td')
                    
                    # print("len(all_table_data_cells) :: ",len(all_table_data_cells))
                    
                    # Exclude the first row
                    if(len(all_table_data_cells) == 0):
                        continue
                    i += 1
                    date_notif = all_table_data_cells[0].text
                    category_notif = title = all_table_data_cells[1].text
                    anchor_tag = all_table_data_cells[2].find('a')
                    title_notif = all_table_data_cells[2].text

                    href_link_notif = anchor_tag.get('href')
                    
                    pdf_link_notif = ""
                    pdf_link_notif = self.get_pdf_url(href_link_notif)

                    new_formatted_date = self.format_date_for_sqlite(date_notif)
                    date_notif = new_formatted_date
                    
                    print(f"Row {i} extracted")
                    i += 1
                    new_row = {
                        "date" : date_notif,
                        "category" : category_notif,
                        "url" : pdf_link_notif,
                        "title" : title_notif,
                    }
                    # print("pdf_link_notif:")
                    # print(pdf_link_notif)
                    if(pdf_link_notif in set_of_most_recent_pdf_links):
                        # print("New Notifs detected")
                        # print(new_notifs)
                        return new_notifs

                    new_notifs.append(new_row)

                ############ ENDS HERE ############
                
                time.sleep(2)

                # Check if the page contains pagination or is it a single page
                if not (driver.find_element(By.CLASS_NAME, "pagination_outer")):
                    print("Currenttly at last page")
                    return

                WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "pagination_outer"))
                    # Check if the pagination_outer is loaded, so we can move on to next step
                )

                # If we cant find the next_button, it means we are at the last page
                if not (driver.find_elements(By.XPATH, "//*[@title='Next']")):
                    print("cant find Next")
                    return

                next_button = driver.find_element(By.XPATH, "//*[@title='Next']")
                time.sleep(2)

                next_button.click()
                logging.info('Clicked on Next Button')

                driver.implicitly_wait(5)
                logging.info('Waited for download to complete')
                
            except Exception as e:
                print("EXCEPTION occurred :",e)
                logging.error(f'An exception occurred: {str(e)}')
                return
            finally:
                # driver.quit()
                # self.save_to_db(new_notifs)
                page_num += 1
        return new_notifs
    
    def download_all_notif_links(self):
        url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingAll=yes"
        data = []
        print("inside download_all_notif_links")
        
        options = webdriver.ChromeOptions()
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument("--headless=new")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        driver = webdriver.Chrome(options=options)
        driver.get(url)
        
        page_num = 0

        while(1):
            print("inside while")
            try:
                print(f"Page : {page_num}")
                # The html contains table which hold info - PDF Title, Data, PDF Viewer Link
                if (page_num > 220):
                    time.sleep(5)
                    ########### GEL HTML/PDF LINKS FROM THE TABLE ###########
                    html_content = driver.page_source
                    soup = BeautifulSoup(html_content,'html.parser')
                    
                    table = soup.find('table')

                    if(table == None):
                        return
                    no_of_rows = len(table.find_all('tr'))

                    print(f"Page : {page_num} | No of rows: {no_of_rows}")
                # ad-hoc arrangement
                    all_rows = table.find_all('tr')
                    if(len(all_rows) == 0):
                        break
                    i = 1
                    for row in all_rows:
                        # print(row)
                        if(row == None):
                            continue
                        all_table_data_cells = row.find_all('td')
                        # Exclude the first row
                        if(len(all_table_data_cells) == 0):
                            continue
                        
                        date_notif = all_table_data_cells[0].text
                        category_notif = title = all_table_data_cells[1].text
                        anchor_tag = all_table_data_cells[2].find('a')
                        title_notif = all_table_data_cells[2].text

                        href_link_notif = anchor_tag.get('href')
                        
                        pdf_link_notif = ""
                        pdf_link_notif = self.get_pdf_url(href_link_notif)

                        print(f"Row {i} extracted")
                        i += 1
                        new_row = {
                            "date" : date_notif,
                            "category" : category_notif,
                            "url" : pdf_link_notif,
                            "title" : title_notif,
                        }

                        data.append(new_row)

                    ############ ENDS HERE ############
                    
                    time.sleep(2)

                    # Check if the page contains pagination or is it a single page
                    if not (driver.find_element(By.CLASS_NAME, "pagination_outer")):
                        print("Currenttly at last page")
                        return

                    WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.CLASS_NAME, "pagination_outer"))
                        # Check if the pagination_outer is loaded, so we can move on to next step
                    )

                    # If we cant find the next_button, it means we are at the last page
                    if not (driver.find_elements(By.XPATH, "//*[@title='Next']")):
                        print("cant find Next")
                        return

                    next_button = driver.find_element(By.XPATH, "//*[@title='Next']")
                    time.sleep(2)

                    next_button.click()
                    logging.info('Clicked on Next Button')

                    driver.implicitly_wait(5)
                    logging.info('Waited for download to complete')
                
                else:
                    print(f"Not getting deep into row : {page_num}")
                    time.sleep(2)
                    
                    # Check if the page contains pagination or is it a single page
                    if not (driver.find_element(By.CLASS_NAME, "pagination_outer")):
                        print("Currenttly at last page")
                        return

                    WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.CLASS_NAME, "pagination_outer"))
                        # Check if the pagination_outer is loaded, so we can move on to next step
                    )

                    # If we cant find the next_button, it means we are at the last page
                    if not (driver.find_elements(By.XPATH, "//*[@title='Next']")):
                        print("cant find Next")
                        return

                    next_button = driver.find_element(By.XPATH, "//*[@title='Next']")
                    time.sleep(2)

                    next_button.click()
                    logging.info('Clicked on Next Button')

                    driver.implicitly_wait(5)
                    logging.info('Waited for download to complete')
                    
            except Exception as e:
                print("EXCEPTION occurred :",e)
                logging.error(f'An exception occurred: {str(e)}')
                return
            finally:
                # driver.quit()
                self.save_to_db(data)
                page_num += 1
                
        print(data)
        self.save_to_db(data)
        
    def create_mapping_type_to_subtype(self):
        
        df = pd.read_csv(globals.urls_of_sebi_menu_csv_path)
        
        mapping_type_to_subtype = {}
        for _,row in df.iterrows():
            type = row['menu']
            subtype = row['submenu']
            mapping_type_to_subtype[subtype] = type
        
        return mapping_type_to_subtype
        
    def store_new_notifs_to_kb(self,new_notifs):
        print("storing new notifs into KB")
        
        mapping_type_to_subtype = self.create_mapping_type_to_subtype()
        to_download = []
        
        for row in new_notifs:

            date = row['date']
            category = row['category']
            url = row['url']
            title = row['title']
            
            # depending on the category(subtype) => find type 
            category = category.lower()
            subtype = category.replace(" ","_")
            type = mapping_type_to_subtype[subtype]
            
            print(f"type and subtype extracted : {type} and {subtype}")
            
            html_link = url
            if(html_link.lower().endswith("pdf")):
                print(html_link)
                pdf_link = html_link
            else:
                pdf_link = self.get_pdf_url(html_link)
            
            split_pdf_link = pdf_link.split("/")
            file_name = split_pdf_link[-1]
                        
            new_row = {
                    "title" : title,
                    "date" : date,
                    "html_link" : html_link,
                    "pdf_link" : pdf_link,
                    "type": type,
                    "sub_type" : subtype,
                    "file_name" : file_name,
                    "pdf_text" : ""
                }
            
            to_download.append(new_row)
        
        print("saving new notifications into csv")
        df2 = pd.DataFrame(to_download)
        df2.to_csv(globals.pdf_links_of_all, mode='a', index=False)
        
        print("downloading new notifs into respective folders")
        # for row in 
        for row in to_download:
            print("Downloading ..... =>", row['pdf_link'])
            self.download_pdf_new(row)
