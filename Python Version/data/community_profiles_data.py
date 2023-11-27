import camelot
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import numpy as np
from itertools import chain
import string
import warnings
import concurrent.futures
import time

def get_all_community_profiles():
    url = "https://www.calgary.ca/communities/profiles.html"
    response = requests.get(url)

    soup = BeautifulSoup(response.content, "html.parser")

    links = set()
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and "/communities/profiles/" in href:
            links.add("https://www.calgary.ca" + href)

    for community_link in links:
        response = requests.get(community_link)
        soup = BeautifulSoup(response.content, "html.parser")
        for link in soup.find_all("a"):
            href = link.get("href")
            if href and "/community-profiles/" in href and ".pdf" in href:
                name = href.strip().split("/")[-1].split(".")[0]
                print(name)
                with open(f"Datasets/community_profiles/{name}.pdf", "wb") as f:
                    f.write(requests.get("https://www.calgary.ca" + href).content)

def create_column_header(arr1, arr2):
    rtn=[]
    for i in arr1:
        for j in arr2:
            rtn.append(i+" ("+j+")")
    return rtn

def read_file(file_name):
    return camelot.read_pdf(f"Datasets/community_profiles/{file_name}", pages='all')

def extract_data(community_name,first_file,tables):
    header=[]
    all_data=[]
    tot_tbl_extracted=0
    for tbl in tables:
        if (tbl.df[0][0].lower()).translate(str.maketrans('', '', string.punctuation)).replace(" ", "")==community_name.replace(".pdf", "").translate(str.maketrans('', '', string.punctuation)).replace(" ", ""):
            tot_tbl_extracted+=1
            if first_file:
                row_as_header = np.delete(tbl.df[0].to_numpy(), [0,1])
                col_as_header = np.delete(tbl.df[:2].to_numpy(),[0])
                header.append(create_column_header(row_as_header,col_as_header[col_as_header.astype(bool)]))
            data=tbl.df.drop([0,1])
            data.drop(data.columns[0], axis=1, inplace=True)
            data=data.values.flatten()
            all_data.append(data.tolist())
    all_data=list(chain.from_iterable(all_data))
    if len(all_data)>0:
        if first_file:
            header=list(chain.from_iterable(header))
            all_data = pd.DataFrame([all_data], columns=[header], index=[community_name])
            all_data.to_csv('all_communities_data.csv')
        else:
            all_data = pd.DataFrame([all_data], index=[community_name])
            all_data.to_csv('all_communities_data.csv', mode='a', header=False)
        print(f"File: {community_name}, total tables extracted: {tot_tbl_extracted}")
        first_file=False

def chunk_array(input_array, max_chunk_size):
    return [input_array[i:i + max_chunk_size] for i in range(0, len(input_array), max_chunk_size)]

# PDF files to extract tables from
with os.scandir("Datasets/community_profiles/") as files:
    start_time = time.time()
    first_file=True
    warnings.filterwarnings('ignore')
    num_threads = 8
    file_names = [file.name for file in files if file.is_file()]
    file_name_chunks=chunk_array(file_names,num_threads)
    for file_name_chunk in file_name_chunks:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_file = {executor.submit(read_file, file_name): file_name for file_name in file_name_chunk}
            for future in concurrent.futures.as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    extract_data(file,first_file,future.result())
                    print(f"Total elapsed Time: {time.time()-start_time} secs")
                    first_file=False
                except Exception as e:
                    print(f"Error extracting data from {file}: {e}")