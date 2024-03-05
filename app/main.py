import data_scraper
import menu_collector
            
def main():
    menu_collectors_obj = menu_collector.SEBIMenuCollector()
    scraper = data_scraper.SEBIDataScraper()
    menu_collectors_obj.collect_menu_links()
    menu_collectors_obj.create_folder_hierarchy()
    list_of_menus = ["about", "legal", "enforcement", "filings", "reports & statistics", "status", "media & notifications"]
    scraper.collect_pdf_links("legal", "acts")
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
    print("Started!!")
    # scraper.download_files3("legal","acts")
    # scraper.download_files3("legal","circulars")
    # scraper.download_pdf("https://www.sebi.gov.in/web/?file=https://www.sebi.gov.in/sebi_data/attachdocs/mar-2023/1679919997585.pdf", r"/home/sarvamuser/law_sarvam/nbs/SEBI_Ankit/SEBI_Extracted_Data/enforcement/orders", "1679919997585.pdf")
    # scraper.download_files3("enforcement","orders")
    # scraper.download_files3("enforcement", "informal_guidance")
    
if __name__ == '__main__':
    main()
    
