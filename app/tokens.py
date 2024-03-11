import tiktoken
import csv
import globals
import sys
import pandas as pd

def find_tokens_of_entire_kb():
    print("counting tokens.....")
    encoding = tiktoken.get_encoding("cl100k_base")

    # num_tokens = len(encoding.encode(string))
    # return num_tokens
    # csv.field_size_limit(sys.maxsize)
    # print(sys.maxsize)
    # limit0 = csv.field_size_limit(sys.maxsize)
    # print("limit of csv ",limit0)
    
    # r = csv.reader(open(globals.kb_of_sebi)) # Here your csv file
    # lines = list(r)
    
    df = pd.read_csv(globals.kb_of_sebi)
    
    token_of_pdf_text = 0
    # Currently, metadata is => title, date
    tokens_of_metadata = 0
    for index,row in df.iterrows():
        # print(index)
        pdf_text = row['pdf_text']
        title = row['title']
        date = row['date']
        
        if(pd.isna(pdf_text) or pd.isna(title) or pd.isna(date)):
            continue
        
        metadata = title + " " + date
        token_of_pdf_text += len(encoding.encode(pdf_text))
        # print(to)
        tokens_of_metadata += len(encoding.encode(metadata))
        
    
    # for row in lines:
    #     if(len(row)!=8):
    #         break
    #     pdf_text = row[6]
    #     # print(pdf_text)
    #     token_of_pdf_text += len(encoding.encode(pdf_text))
    #     metadata = row[0] + " " + row[1]
    #     tokens_of_metadata += len(encoding.encode(metadata))
    
    print("Tokens of Text : ",token_of_pdf_text)
    print("Tokens of metadata(title and date) : ",tokens_of_metadata)
    
    return token_of_pdf_text,tokens_of_metadata

find_tokens_of_entire_kb()

# def num_tokens_from_string(string: str, encoding_name: str) -> int:
#     encoding = tiktoken.get_encoding(encoding_name)
#     num_tokens = len(encoding.encode(string))
#     return num_tokens

# print(num_tokens_from_string("Hello world, let's test tiktoken.", "cl100k_base"))