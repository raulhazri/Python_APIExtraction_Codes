#!/usr/bin/env python
# coding: utf-8

# In[1]:


def do_run_binapr():
    import os
    import sharepy
    from pandas import DataFrame
    import pandas as pd
    import csv
    import psycopg2
    import sqlalchemy
    from sqlalchemy import create_engine
    import tabula
    import logging
    from shareplum import Office365
    from shareplum import Site
    from shareplum.site import Version

    s = sharepy.connect(site='https://cityofyarra.sharepoint.com', username='ycc.automation@yarracity.vic.gov.au', password='Winter@2021#')
    site = "https://cityofyarra.sharepoint.com/sites/CityworksDepot/S&I/"
    library = "Bin Contamination APR"



        #Get list of all files and folders in library
    files = s.get("{}/_api/web/lists/GetByTitle('{}')/items?$select=FileLeafRef,FileRef"
                    .format(site, library)).json()["d"]["results"]
    for file in files:
        source = "https://" + s.site + file["FileRef"]
        dest = "." + file["FileRef"]
            # Check if item is file or folder
        folder = s.get("{}/_api/web/GetFolderByServerRelativeUrl('{}')"
                        .format(site, file["FileRef"])).status_code == 200
        #print(source)



        if not os.path.exists(dest):
            if folder:
                os.makedirs(dest)  # Create folders
            else:
                s.getfile(source, filename=dest)  # Download files



        #Fetch pdf files from folder
    arr1 = os.listdir('/Users/ycc.automation/sites/CityworksDepot/S&I/Bin Contamination APR/To Be Processed')
        #print(arr1)
    arr = list(filter(lambda k: '.pdf' in k,arr1))



        #path for pdf files
    str = '/Users/ycc.automation/sites/CityworksDepot/S&I/Bin Contamination APR/To Be Processed/'
    file = [str + x for x in arr]
    #print(file)

    authcookie1 = Office365('https://cityofyarra.sharepoint.com', username='ycc.automation@yarracity.vic.gov.au', password='Winter@2021#').GetCookies()
    site1 = Site('https://cityofyarra.sharepoint.com/sites/CityworksDepot/S&I/', version=Version.v365, authcookie=authcookie1);
    folder1 = site1.Folder('Python Processed Files/Bin Contamination APR Procesed')
    folder2 = site1.Folder('Bin Contamination APR/To Be Processed')
        #for loop and  dataframes
    for i in file:
        table = tabula.read_pdf(i,pages="all", multiple_tables = True)
        table1 = table[0] #To read the first table from pdf
        df = pd.DataFrame(table1)
        df = df.columns.to_frame().T.append(df, ignore_index=True)
        df.columns = range(len(df.columns)) #To make header as row
            #print (df)
        table2 = table[1] #To read the second table
        table2
        df1 = pd.DataFrame(table2)
        df1 = df1.columns.to_frame().T.append(df1, ignore_index=True)
        df1.columns = range(len(df1.columns)) #To make the header as row
            #print (df1)
        table3 = table[2] #To read the third table
        df2 = pd.DataFrame(table3)
        df2 = df2.columns.to_frame().T.append(df2, ignore_index=True)
        df2.columns = range(len(df2.columns)) #To make the header as row
            #print (df2)
        table4 = table[3] #To read the fourth table
        df3 = pd.DataFrame(table4)
        df3 = df3.columns.to_frame().T.append(df3, ignore_index=True)
        df3.columns = range(len(df3.columns)) #To make the header as row
        dfm=  pd.concat([df, df1, df2, df3], ignore_index=True) #To combine all the tables
        dfm = dfm.T #To transpose the table 
            #print(dfm)
        dfm.columns=dfm.iloc[0]
        dfm=dfm[1:] #To make the first row as header
            #To rename non supported column names
        dfm1 = dfm.rename(columns={"TOTAL LOAD WEIGHT (Tonnes):":"TOTAL LOAD WEIGHT TONNES", "CONTAMINATION (%):":"CONTAMINATION", "CONTAMINATION CHARGE: (GST NOT INCL.)": "CONTAMINATION CHARGE", "GREEN WASTE (SOIL, TREES, GRASS ETC):": "GREEN WASTE", "RUBBISH/ FOOD:": "RUBBISH_FOOD"})
            #To extract the required information
        dfmf = dfm1.loc[:, dfm1.columns.isin(['DATE RECEIVED:', 'CUSTOMER:', 'TRANSPORT:','TRUCK REGISTRATION:','ZONE NUMBER & RUN NUMBER','WEIGHBRIDGE DOCKET NUMBER:','INSPECTED BY:','TOTAL LOAD WEIGHT TONNES','SITE TIPPED AT:','CONTAMINATION','CONTAMINATION CHARGE','TIME:','HARD RUBBISH:','GREEN WASTE','GLASS:','RUBBISH_FOOD','MEDICAL CONTAMINANTS:'])]
            #print(dfmf)
            #load the data into postgresql database
        engine = sqlalchemy.create_engine('postgresql://postgres:gvA1gXx7YQZmVZHzANHE@localhost:5432/SI_Automation')
        con=engine.connect()
        dfmf.to_sql('Bin_Contamination_APR',con,if_exists='append',index=False)
        con.close()
            #To move the files from one folder to another
        i1 = i.split('/')[-1]
        with open(i, mode='rb') as file1:
            fileContent = file1.read()
        folder1.upload_file(fileContent,i1)
        folder2.delete_file(i1)
        #To delete the files from python directory
        os.remove(i)


# In[ ]:


import schedule
import time
schedule.every().day.at("16:15").do(do_run_binapr)
while True:
    schedule.run_pending()
    time.sleep(1)


# In[ ]:




