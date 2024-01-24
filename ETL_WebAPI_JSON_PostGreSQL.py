#!/usr/bin/env python
# coding: utf-8

# In[1]:


def do_run_tripdetails():
    import requests
    from requests.auth import HTTPBasicAuth
    import json
    import pandas as pd
    import time
    import psycopg2
    from psycopg2.extras import execute_values
    import requests
    from pandas import DataFrame
    import sqlalchemy
    from sqlalchemy import create_engine
    import time
    from datetime import datetime, date, time, timedelta
    now = datetime.now()
    today_now = now.replace(hour = 0, minute = 0, second = 0)
    today = today_now.strftime("%d-%m-%Y %H:%M:%S")
    #today
    yesterday = (datetime.now() - timedelta(1))
    yesterday_now = yesterday.replace(hour = 0, minute = 0, second = 0)
    yesterday = yesterday_now.strftime("%d-%m-%Y %H:%M:%S")
    #yesterday
    tkn_payload={}
    tkn_headers = {}
    tokenurl =  "https://local-government.street-directory.com.au/index.php/api_users/login?username=APIYarra&password=aPi4724"
    headers = {}
    payload = {}
    # To extract device ID
    device_data_devices = requests.get('https://svr15.street-directory.com.au/apis/tracking/devices.php?username=APIYarra&password=aPi4724').json()
    device_data_devices
    device_ids = [value['device_id'] for value in device_data_devices['data']]
    for i in device_ids:
        token = requests.request("GET", tokenurl, headers=tkn_headers, data=tkn_payload).json()
        token1= token['token']
        url = "https://local-government.street-directory.com.au/index.php/report/generate/fbt_from_events/"+token1+"?date_time_range[start]="+yesterday+"&date_time_range[end]="+today+"&join_tab=1&devices[]="+i+"&min_duration=0&mask_address=0&page=1&perpage=100000"
        response = requests.request("GET", url, headers=headers, data=payload)
        response_dict = json.loads(response.text)
        #print(response_dict)
        response1=response_dict['data']
        #print(response1)
        for j in response1:
            tripno = str(j['no'])
            rego = str(j['vehicle_reg'])
            driver = str(j['driver'])
            starttime = str(j['start_time'])
            startroadname = str(j['start_roadname'])
            startodo = str(j['starting_odo'])
            endtime = str(j['end_time'])
            endroadname = str(j['end_roadname'])
            distance = str(j['distance'])
            purpose = str(j['purpose'])
            start_lat = str(j['__metadata']['start']['alat'])
            start_long = str(j['__metadata']['start']['along'])
            driverid = str(j['__metadata']['start']['driver_id'])
            drivername = str(j['__metadata']['start']['driver_name'])
            end_lat = str(j['__metadata']['end']['alat'])
            end_long = str(j['__metadata']['end']['along'])
            endodo = str(j['__metadata']['end']['mileage'])
            try:
                endgeo = j['__metadata']['end']['in_geofences']
                endgeo = str(j['__metadata']['end']['in_geofences'])
            except KeyError:
                j['__metadata']['end']['in_geofences'] = None
                endgeo = str(j['__metadata']['end']['in_geofences'])
            try:
                startgeo = j['__metadata']['start']['in_geofences']
                startgeo = str(j['__metadata']['start']['in_geofences'])
            except KeyError:
                j['__metadata']['start']['in_geofences'] = None
                startgeo = str(j['__metadata']['start']['in_geofences'])
            realtime = []
            realtime += [tripno]
            realtime += [rego]
            realtime += [driver]
            realtime += [starttime]
            realtime += [startroadname]
            realtime += [startodo]
            realtime += [endtime]
            realtime += [endroadname]
            realtime += [distance]
            realtime += [purpose]
            realtime += [start_lat]
            realtime += [start_long]
            realtime += [driverid]
            realtime += [drivername]
            realtime += [end_lat]
            realtime += [end_long]
            realtime += [endodo]
            realtime += [endgeo]
            realtime += [startgeo]
            df = DataFrame ([realtime],columns=['tripno','rego','driver','starttime','startroadname','startodo','endtime','endroadname','distance','purpose','start_lat','start_long','driverid','drivername','end_lat','end_long','end_odo','endgeo','startgeo'])
            #df
            engine = sqlalchemy.create_engine('postgresql://postgres:gvA1gXx7YQZmVZHzANHE@localhost:5432/Intellitrac_Data')
            con=engine.connect()
            df.to_sql('trip_details_intellitrac_2',con,if_exists='append',index=False)
            con.close()


# In[ ]:


import schedule
import time
schedule.every().day.at("12:00").do(do_run_tripdetails)
while True:
    schedule.run_pending()
    time.sleep(1)


# In[ ]:




