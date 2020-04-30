import pandas as pd
import os
import re
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from concurrent.futures import thread, wait
from urllib.request import urlretrieve


#Cleaning dataframe for naming file
def clean_title(df):
    # Select all duplicate rows based on one column
    df_dp = df[df.duplicated(['Book Title'], keep=False)].applymap(str) 
    #Extracting the edition
    df_dp['Edition'] = df_dp.loc[:,'Edition'].str[:-6]
    #Book title with edition
    df_dp['Book Title'] = df_dp['Book Title'] + ' ' + df_dp['Edition'] 
    df_str = df.copy().applymap(str)
    #Combine the dataframe containing new title with the old data frame and remove the duplicates from old data frame
    df_str= pd.concat([df_str, df_dp]).drop_duplicates(['S.No.'], keep='last').sort_values('S.No.') 
    #Add back the year for the edition (optional)
    df_str['Edition'] = df['Edition']
    df = df_str.sort_index() 
    #Lastly, we deal with exception of naming files in windows
    df['Book Title'] = df['Book Title'].str.replace('/',' ').str.replace(':',' ')
    return df

#Print all Book title with corresponding index and total books
def print_titles(df, column_index):
    for ind in range(df.shape[0]):
        print(ind, '\t', df.iloc[ind, column_index])
    print("Total items:", df.shape[0])

#Search for certain word in dataframe
def word_search(label, df):  
    temp_df = df[df['Book Title'].str.contains(label)] 
    return temp_df

#Changing number range into numbers
def num_range(range_str):
    range_str = range_str.replace(' ', '')

    nums = set()
    for s in range_str.split(','):
        match = re.match(r'(\d+)-(\d+)', s)
        if match:
            for i in range(int(match.group(1)), int(match.group(2)) + 1):
                nums.add(i)
        else:
            nums.add(int(s))
    return nums

#Search for certain number
def num_search(label, df): 
    row = []
    for i in num_range(label):
        if 0 <= i < df.shape[0]:
            row.append(i)
        temp_df = df.iloc[row,:]
    return temp_df

#Search for certain values or characters
def search(df):
    list_of_df = []
    list_of_df.append(df)
    
    while True:
        curr_df = list_of_df[-1]
        print_titles(curr_df, 1)
        label = input('Download by pressing enter, Previous search ";", Back to main "/" , or Select number(0-3,5) or keyword(Ele): \n')  
        
        if  label == '':
            break 
        elif label == ';':
            if len(list_of_df) > 1: list_of_df.pop()
        elif label == '/':
            list_of_df.append(df)
        elif ((',' in label) or ('-' in label) or (label.isnumeric())) and not bool(re.match('^(?=.*[a-zA-Z])', label)):
            result_df = num_search(label, curr_df)
            list_of_df.append(result_df)
        else:
            result_df = word_search(label, curr_df)
            list_of_df.append(result_df)
    
    return  list_of_df[-1]
    
    
#Handling exception
def download(url, save_path): 
    try:
        urlretrieve(url, save_path)
    except:
        print(url)

#Download all pdf, specify number of max worker, folder location and the data frame containing titles and link
def download_pdf(df, folder_location, max_worker): 
    #Number of worker(downloader)
    MAX_WORKERS = max_worker
    fs = []
    executor = thread.ThreadPoolExecutor(MAX_WORKERS)
    if not os.path.exists(folder_location):os.mkdir(folder_location)
        
    for ind in df.index:
        url = df['OpenURL'][ind]
        name = df['Book Title'][ind] + ".pdf"
        response = requests.get(url, stream = True)
        soup= BeautifulSoup(response.text, "html.parser")
        address = soup.select("a[href$='.pdf']")[0]['href']
        save_path = os.path.join(folder_location,name)
        download_url = urljoin(url, address)
        future = executor.submit(download, download_url, save_path)
        fs.append(future)
    wait(fs)

#Main Program

excel_loc = input('Type your Springer excel location (ex: C:\\Users\\User\\Desktop\\Springer\\Springer Ebooks.xlsx) in your PC: \n')
df = pd.read_excel(excel_loc)
#Change 1st row into column header
new_header = df.iloc[0] 
df = df[1:] 
df.columns = new_header

#Cleaning title name
clean_df = clean_title(df)

#Sort Based on Book Title
sort_df = clean_df.sort_values('Book Title').drop('S.No.', axis=1).reset_index().drop('index', axis=1).reset_index()

while True:
    #Search for books you like to download
    result_df = search(sort_df)

    #Input your folder location
    folder_location = input('Type your location (ex: C:\\Users\\User\\Desktop\\Springer) to store all your pdf file: \n')

    #Download all the pdf by specifying number of worker for your pdf
    download_pdf(result_df, folder_location, 10) 
    print("Finish downloading all your file")
    if  input("Finish program by pressing enter, continue download by typing other character") == '': break
