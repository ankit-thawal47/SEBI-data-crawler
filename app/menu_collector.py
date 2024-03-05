import globals
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd


class SEBIMenuCollector:
    
        def dictify(self,ul):
            result = {}
            for li in ul.find_all("li", recursive=False):
                key = next(li.stripped_strings)
                url = ""
                a_tags = li.find_all('a')
                if(len(a_tags) == 1):
                    url = a_tags[0]['href']
                ul = li.find("ul")
                if ul:
                    result[key] = self.dictify(ul)
                else:
                    if(not url.startswith("http")):
                        base_url = "https://www.sebi.gov.in"
                        joined_url = urljoin(base_url, url)
                        url = joined_url
                    result[key] = url
            return result
        
        def download_menus_js(self):
            menus_of_sebi = "D:\Educational\Sarvam AI\Python\SEBI_Ankit\menus_of_sebi.html"
            import requests
            menus_of_sebi = "https://www.sebi.gov.in/js/menu.js"
            response = requests.get(menus_of_sebi)
            html_content = response.text

            html_content = html_content.replace('document.write("',"")
            html_content = html_content.replace('");','')
            html_content = "<html>" + html_content + "</html>"
            return html_content
        
        def create_folder_hierarchy(self):
            try:
                df = pd.read_csv(globals.urls_of_sebi_menu_csv_path)

                # Base folder to store SEBI Extracted Data
                print("Creating Folder Hierarchy for : ",globals.base_folder_path)
                if not os.path.exists(globals.base_folder_path):
                    # Create the base folder if it doesn't exist
                    os.makedirs(globals.base_folder_path)

                for _,row in df.iterrows():
                    menu = row['menu']
                    menu = menu.replace(" ","_")
                    menu = menu.lower()
                    print(menu)
                    menu_folder = os.path.join(globals.base_folder_path,menu)
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

        def collect_menu_links(self):
            if(os.path.exists(globals.urls_of_sebi_menu_csv_path)):
                return

            html_content = self.download_menus_js()
            
            # Parse the HTML
            soup = BeautifulSoup(html_content, "html.parser")

            # Get the main list
            ul = soup.ul
            result = self.dictify(ul)

            # Example data dictionary
            data_dict = result

            # Initialize empty lists for each column
            menu_list, submenu_list, description_list, url_list = [], [], [], []

            # Iterate over the data dictionary and append values to the lists
            for menu, submenu_dict in data_dict.items():
                for submenu, details in submenu_dict.items():
                    if isinstance(details, dict):
                        for description, url in details.items():
                            menu_list.append(menu.lower())
                            submenu_list.append(submenu.lower())
                            description_list.append(description.lower())
                            url_list.append(url)
                    else:
                        menu_list.append(menu.lower())
                        submenu_list.append(submenu.lower())
                        description_list.append("")
                        url_list.append(details)

            # Create DataFrame from lists
            df = pd.DataFrame({
                'menu': menu_list,
                'submenu': submenu_list,
                'subsubmenu': description_list,
                'url': url_list
            })
            
            row_count = len(df)
            for i in range(row_count):
                row = df.iloc[i]
                menu = row['menu']
                submenu = row['submenu']
                subsubmenu = row['subsubmenu']
                print(menu, submenu, subsubmenu)
                df.at[i, 'menu'] = menu.replace(" ","_")
                df.at[i, 'submenu'] = submenu.replace(" ","_")
                df.at[i, 'subsubmenu'] = subsubmenu.replace(" ","_")
            print("replaced <space> with <_>")

            '''df2 maintains historical_data links'''
            df2 = pd.DataFrame(
                {
                    'menu': ["legal"] * len(globals.legal_menu_and_sub_menu),
                    'submenu': list(globals.legal_menu_and_sub_menu.keys()),
                    'subsubmenu': ["historical_data"] * len(globals.legal_menu_and_sub_menu),
                    'url': list(globals.legal_menu_and_sub_menu.values())
                }
            )

            final_df = pd.concat([df, df2], axis=0)
            final_df.to_csv(globals.urls_of_sebi_menu_csv_path, mode='a')
        
        # def collect_menu_links(self):
        # # These are hardcoded menulist for "Historical Data"
        #     if(os.path.exists(globals.urls_of_sebi_menu_csv_path)):
        #         return
        #     legal_menu_and_sub_menu = globals.legal_menu_and_sub_menu

        #     menus_of_sebi = globals.menus_of_sebi

        #     html_content = ""
        #     # Read the HTML content from the file
        #     with open(menus_of_sebi, "r") as file:
        #         html_content = file.read()

        #     # Parse the HTML
        #     soup = BeautifulSoup(html_content, "html.parser")

        #     # Get the main list
        #     ul = soup.ul
        #     result = self.dictify(ul)

        #     # Example data dictionary
        #     data_dict = result

        #     # Initialize empty lists for each column
        #     menu_list, submenu_list, description_list, url_list = [], [], [], []

        #     # Iterate over the data dictionary and append values to the lists
        #     for menu, submenu_dict in data_dict.items():
        #         for submenu, details in submenu_dict.items():
        #             if isinstance(details, dict):
        #                 for description, url in details.items():
        #                     menu_list.append(menu.lower())
        #                     submenu_list.append(submenu.lower())
        #                     description_list.append(description.lower())
        #                     url_list.append(url)
        #             else:
        #                 menu_list.append(menu.lower())
        #                 submenu_list.append(submenu.lower())
        #                 description_list.append("")
        #                 url_list.append(details)

        #     # Create DataFrame from lists
        #     df = pd.DataFrame({
        #         'menu': menu_list,
        #         'submenu': submenu_list,
        #         'subsubmenu': description_list,
        #         'url': url_list
        #     })
            
        #     row_count = len(df)
        #     for i in range(row_count):
        #         row = df.iloc[i]
        #         menu = row['menu']
        #         submenu = row['submenu']
        #         subsubmenu = row['subsubmenu']
        #         print(menu, submenu, subsubmenu)
        #         df.at[i, 'menu'] = menu.replace(" ","_")
        #         df.at[i, 'submenu'] = submenu.replace(" ","_")
        #         df.at[i, 'subsubmenu'] = subsubmenu.replace(" ","_")
        #     print("replaced <space> with <_>")

        #     df2 = pd.DataFrame(
        #         {
        #             'menu': ["legal"] * len(legal_menu_and_sub_menu),
        #             'submenu': list(legal_menu_and_sub_menu.keys()),
        #             'subsubmenu': ["historical_data"] * len(legal_menu_and_sub_menu),
        #             'url': list(legal_menu_and_sub_menu.values())
        #         }
        #     )

        #     final_df = pd.concat([df, df2], axis=0)
        #     final_df.to_csv(globals.urls_of_sebi_menu_csv_path, mode='a')
            
   