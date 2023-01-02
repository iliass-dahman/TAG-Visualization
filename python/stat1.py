from cassandra.cluster import Cluster
import pandas as pd
import math
import warnings
warnings.filterwarnings('ignore')





from datetime import datetime
from datetime import timedelta
import pandas as pd
import time



def process_data_1(users_data,date,update):



            users = users_data['id_user'].unique()
            
            #get the old users 

            old_users =  pd.DataFrame(list(session.execute(f"SELECT id_user FROM Event \
            where timestamp < '{date}' ALLOW FILTERING;")))

            try:
                old_users = old_users['id_user'].unique()
            except:
                old_users = []

            #find the new users
            #new_users_count = 0
            new_users = []
            for user in users:
                if user not in old_users:
                    new_users.append(user)


            details = {"month":[],"year":[]}

            #find the type of subsciption for new users
            prepared_statement = session.prepare('SELECT type from Card where id_client = ? ALLOW FILTERING;')

            for user in new_users:
                card_type = pd.DataFrame(list(session.execute(prepared_statement,[user])))['type'][0]
                if(card_type == "Month"):
                    details['month'].append(user)
                elif(card_type == "Year"):
                    details['year'].append(user)

            short_date = str(datetime.strptime(str(date), "%Y-%m-%d")).split(" ")[0]
            

            if(update==0):
                
                session.execute(f"INSERT INTO statistics_1(day, month_user, new_subs, year_user,LPRD)\
                VALUES ('{short_date}', {len(details['month'])},{len(new_users)} ,{len(details['year'])},'{max(users_data.loc[:,'timestamp'])}');")
                
            else:
                NS=int(pd.DataFrame(list(session.execute(f'SELECT new_subs FROM statistics_1 where day=\'{short_date}\';')))['new_subs'][0]) + len(new_users)
                MU=int(pd.DataFrame(list(session.execute(f'SELECT month_user FROM statistics_1 where day=\'{short_date}\';')))['month_user'][0]) + len(details['month'])
                YU=NS-MU
                session.execute(f"UPDATE statistics_1 SET \
                    month_user ='{MU}' ,new_subs ={NS},year_user= {YU},LPRD='{users_data.loc[(users_data.shape[0]-1),'timestamp']}'\
                        WHERE day = '{short_date}';")
            
            print("statisctics saved!")



def statistics_1(session):
    #get the max date for which data was processed
    
    last_day = pd.DataFrame(list(session.execute('SELECT max(day) as last_day FROM "statistics_1";')))['last_day'][0]
    
    
    #print(last_day)


    if last_day != None:
        last_day = last_day.split(" ")[0]
        #get the the timestamp for the last processed record
        last_record_ts = pd.DataFrame(list(session.execute(f'SELECT max(LPRD) as last_record FROM "statistics_1" where \
            day=\'{last_day}\' allow filtering;')))['last_record'][0]

        #see if it remains greater timestamps for the same day
        #2022-11-08T21:46:40.238+0000
        
        date_1 = datetime.strptime(last_day, "%Y-%m-%d")
        end_date = date_1 + timedelta(days=1)

        data = pd.DataFrame(list(session.execute(f"SELECT id_user FROM Event where \
            timestamp > '{last_record_ts}' and  timestamp < '{end_date}' allow filtering;")))

        if(data.shape[0]>0):
            process_data_1(data,date=last_record_ts,update=1)


        #get the users for the new day
        #date_1 = datetime.strptime(str(last_day), "%Y-%m-%d")
        #end_date = date_1 + timedelta(days=1)
        #end_date = time.mktime(datetime.datetime.strptime(end_date, "%Y/%m/%d").timetuple())
    
        dates = pd.DataFrame(list(session.execute(f"SELECT toDate(timestamp) as date\
         FROM Event where timestamp >= '{end_date}' allow filtering;")))
        if(len(dates.columns) == 0):
            dates = []
        else:
            dates = dates['date'].unique()
    else:
        dates = pd.DataFrame(list(session.execute(f"SELECT toDate(timestamp) as date\
         FROM Event;")))['date'].unique()


    dates.sort()
    
    for date in dates:

           

            #date_time_obj = datetime. strptime(str(date), '%Y-%m-%d')

            #if (date_time_obj.date() >= datetime.now().date()):
            #    print(f"you have to wait until next day to process {date_time_obj.date()} data")
            #    return
            #print(date)
            #date = datetime.fromtimestamp(date).date()
            date_1 = (datetime.strptime(str(date), "%Y-%m-%d"))
            end_date = date_1 + timedelta(days=1)
            end_date = str(end_date.date())
            date = str(date)
            print(f"date --> {date}")
            #print(end_date.date())
            #end_date = int(time.mktime(datetime.strptime(str(end_date.date()), "%Y-%m-%d").timetuple()))
            #start =  int(time.mktime(datetime.strptime(str(date), "%Y-%m-%d").timetuple()))
            #print(int(start))
            #print(int(end_date))
            #print(date)
            #print(end_date)
            users =  pd.DataFrame(list(session.execute(f"SELECT id_user,timestamp FROM Event where \
            timestamp >= '{date}' and  timestamp < '{end_date}' allow filtering;")))
            #print(users)
            
            process_data_1(users,date,update=0)
    print("Processing Streaming data.........")
    while(True):

        #Every 5 seconds, check if there is a new data
        time.sleep(5)

        #get the last timestamp processed 
       
        last_timestamp_processed = pd.DataFrame(list(session.execute(f'SELECT max(LPRD) as last_record FROM "statistics_1";')))['last_record'][0]
        
        new_data = pd.DataFrame(list(session.execute(f"SELECT * FROM Event where \
            timestamp > '{last_timestamp_processed}' allow filtering;")))

        if(new_data.shape[0] > 0): #if there is new data

            for i in range(new_data.shape[0]):

                #get the timestamp of that data

                record_ts = new_data.loc[i,"timestamp"]
                
                record_ts_1 = str(record_ts).split(" ")[0]
                #check if record_ts has been already in statistics_1

                date_1 =datetime.strptime(record_ts_1, "%Y-%m-%d").split(" ")[0]
                #end_date = str(date_1.date())
                
                ts_in_stat1 = pd.DataFrame(list(session.execute(f'SELECT day FROM "statistics_1" where day=\'{end_date}\' allow filtering;')))
                
                if(len(ts_in_stat1.columns) == 0):
                    ts_in_stat1 = None
                else:
                    ts_in_stat1 = ts_in_stat1['day'][0]
                
                query =  session.prepare('SELECT type from Card where id_client = ? ALLOW FILTERING;')

            
                card_type = pd.DataFrame(list(session.execute(query,[new_data.loc[i,"id_user"]])))['type'][0]
                
                if(ts_in_stat1!=None):
                    #day exists in statistics_1
                    

                    if(card_type == "Month"):

                        NS=int(pd.DataFrame(list(session.execute(f'SELECT new_subs FROM "statistics_1 where day=\'{end_date}\';')))['new_subs'][0]) + 1
                        MU=int(pd.DataFrame(list(session.execute(f'SELECT month_user FROM "statistics_1 where day=\'{end_date}\';')))['month_user'][0]) + 1
                        session.execute(f"UPDATE statistics_1 SET \
                        month_user ={MU} ,new_subs ={NS},LPRD='{record_ts}'\
                            WHERE day = '{date_1}';")
                        print(f"new record have been processed {record_ts}")
                    else:
                        NS=int(pd.DataFrame(list(session.execute(f'SELECT new_subs FROM statistics_1 where day=\'{end_date}\';')))['new_subs'][0]) + 1
                        YU=int(pd.DataFrame(list(session.execute(f'SELECT year_user FROM statistics_1 where day=\'{end_date}\';')))['year_user'][0]) + 1
                        session.execute(f"UPDATE statistics_1 SET \
                        new_subs ={NS},year_user= {YU},LPRD='{record_ts}'\
                            WHERE day = '{date_1}';")   
                        print(f"new record have been processed {record_ts}")
                    
                else:
                    #it's a new day
                    if(card_type == "Month"):

                        session.execute(f"INSERT INTO statistics_1(day, month_user, new_subs, year_user,LPRD)\
                VALUES ('{date_1}', 1,1 ,0,'{record_ts}');")
                        print(f"new record have been processed {record_ts}")
                    else:
                        session.execute(f"INSERT INTO statistics_1(day, month_user, new_subs, year_user,LPRD)\
                VALUES ('{date_1}', 0,1 ,1,'{record_ts}');")
                        print(f"new record have been processed {record_ts}")













                

if __name__ == "__main__":

    print("Connecting to the Cluster..........")
    #connect to the cluster
    cluster = Cluster(["127.0.0.1"],port=9042)


    #connect to the keyspace
    session = cluster.connect('test')

    print("generating type 1 statictics.........")

    statistics_1(session)







# when executing the script
#Statistics_1
    #get the last date which for the data was processed
        #if exists 
            #get the last timestamp(t1) of processed data for the current date from statistics_1
            #get all timestamps from event table which are greater than t1 for the current day --> process them 
            #pass to the next day

        #if doesn't exist
            #process data from the begenning
    #1.get all new records starting from the last record processed for that day
    #2.process them all
    #3.process the stream of records until stopping the scipt
