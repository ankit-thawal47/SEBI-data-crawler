

from bs4 import BeautifulSoup
import globals
import time
import csv

def extract_data_from_html(file_path):
    html_content = ""
    with open(file_path,"r+") as f:
        html_content = f.read()
    soup = BeautifulSoup(html_content, 'html.parser')

    text = soup.get_text(separator='\n', strip=True)
    return text

import pytesseract
from pdf2image import convert_from_path
import os
import fitz
def extract_data_from_pdf(file_path):
    
    text = ''
    with fitz.open(file_path) as doc:
        for page in doc:
            text += page.get_text()
    return text


def extract_data():
    "extracting data from pdf and htmls into csv"
    try:
        
        r = csv.reader(open(globals.pdf_links_of_all)) # Here your csv file
        lines = list(r)
        
        print(lines[0])
        
        for index,row in enumerate(lines):
            try:
                print(index)
                
                if(index == 0):
                    continue
                
                if(index%2000 == 0):
                    time.sleep(30)
                
                # ['title', 'date', 'html_link','pdf_link', 'type', 'sub_type', 'file_name', 'pdf_text' ]
                pdf_link = row[3]
                html_url = row[2]
                title = row[0]
                type = row[4]
                sub_type = row[5]
                file_name = row[6]
                
                def create_download_path(type, sub_type):
                    type = type.replace(" ","_")
                    sub_type = sub_type.replace(" ","_")
                    
                    type_folder_path = os.path.join(globals.SEBI_data_extraction_base_folder,type)
                    sub_type_folder_path = os.path.join(type_folder_path,sub_type)
                    download_path = sub_type_folder_path
                    return download_path
                    
                download_path = create_download_path(type,sub_type)
                file_path = os.path.join(download_path,file_name)
                
                if(not os.path.exists(file_path)):
                    continue
                
                
                text = ""
                if(file_name.endswith("pdf")):
                    text = extract_data_from_pdf(file_path)
                elif(file_name.endswith("html")):
                    text = extract_data_from_html(file_path)
                
                
                row[7] = text
            except Exception as e:
                print(e)
            # print(row[7])
            print(type,sub_type)
            
        
        with open(globals.kb_of_sebi, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(lines)
        
        print(globals.kb_of_sebi)
    
    except Exception as e:
        print(e)