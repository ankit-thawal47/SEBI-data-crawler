import os

domain = "SEBI"

home_page_url = "https://www.sebi.gov.in"

regChat_folder = r"D:\Educational\Sarvam_AI\Projects\SEBI_Ankit\SEBI-data-crawler-csv"
extraction_folder_name = "SEBI_Extracted_Data"

base_folder_path = os.path.join(regChat_folder,extraction_folder_name)

SEBI_data_extraction_base_folder = os.path.join(regChat_folder,extraction_folder_name)

urls_of_sebi_menu_csv_path = os.path.join(regChat_folder,"urls_of_menus_of_sebi.csv")
pdf_links_of_all = os.path.join(regChat_folder,"pdf_urls_of_all_files_of_sebi.csv")
columns_for_pdf_links_of_all = ['title', 'date', 'html_link','pdf_link', 'type', 'sub_type', 'file_name', 'pdf_text' ]

legal_menu_and_sub_menu = {
                "acts" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingLegal=yes&sid=1&ssid=1&smid=0",
                "rules" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingLegal=yes&sid=1&ssid=2&smid=0",
                "regulations" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingLegal=yes&sid=1&ssid=3&smid=0",
                "general_orders" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=4&smid=0",
                "guidelines" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=5&smid=0",
                "master_circulars" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=6&smid=0",
                "circulars" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0",
                # Doubt ? => How is Circulars are Circular_archive are different?
                "circulars_archive" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingCirArchive=yes&sid=1&ssid=7&smid=0",
                "gazette_notification" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=82&smid=0",
                "online_application_portal" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=91&smid=0",
                "guidance_notes" : "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=85&smid=0",
            }

menus_of_sebi = "/home/sarvamuser/law_sarvam/nbs/SEBI_Ankit/menus_of_sebi.html"