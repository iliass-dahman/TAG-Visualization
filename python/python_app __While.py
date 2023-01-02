from cassandra.cluster import Cluster
import pandas as pd
import math
import warnings
warnings.filterwarnings('ignore')


#connect to the cluster
#cluster = Cluster(['docker-cassandra-1'],port=9042)
print("connecting to cassandra cluster.........")
cluster = Cluster(["127.0.0.1"],port=9042)


#connect to the keyspace
session = cluster.connect('test')



from datetime import datetime
from datetime import timedelta
import pandas as pd
import time

# when executing the script
#Statistics_1
    #get the last date which for the data was processed
        #if exists 
            #1.get the last timestamp(t1) of processed data for the current date from statistics_1
            #2.get all timestamps from event table which are greater than t1 for the current day --> process them 
            #3.While is still dates
                #Do 2
                #pass to the next day
            #4.foreach new record 
                #Do 2
        #if doesn't exist
            #5.While is still dates
                #Do 2
                #pass to the next day
            #6.foreach new record 
                #Do 2




def statistics_1(session):
    #get the max date for which data was processed
    last_day = pd.DataFrame(list(session.execute('SELECT max(day) as last_day FROM "statistics_1";')))['last_day'][0]
    #print(last_day)
    if last_day != None:

        #get the the timestamp for the last processed record
        last_record_ts = pd.DataFrame(list(session.execute(f'SELECT max(LPRD) as last_record FROM "statistics_1" where \
            day={last_day} allow filtering;')))['last_record'][0]

        #see if it remains greater timestamps for the same day
        #2022-11-08T21:46:40.238+0000

        date_1 = datetime.strptime(str(last_day), "%Y-%m-%d")
        end_date = date_1 + timedelta(days=1)

        data = pd.DataFrame(list(session.execute(f"SELECT id_user FROM Event where \
            timestamp >= '{last_record_ts}' and  timestamp < '{end_date}' allow filtering;")))

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
            #print(end_date.date())
            #end_date = int(time.mktime(datetime.strptime(str(end_date.date()), "%Y-%m-%d").timetuple()))
            #start =  int(time.mktime(datetime.strptime(str(date), "%Y-%m-%d").timetuple()))
            #print(int(start))
            #print(int(end_date))
            #print(date)
            #print(end_date)
            users =  pd.DataFrame(list(session.execute(f"SELECT id_user FROM Event where \
            timestamp >= '{date}' and  timestamp < '{end_date}' allow filtering;")))
            #print(users)
            
            process_data_1(users,date,update=0)

    while(True):

        #Every 5 seconds, check if there is a new data
        time.sleep(5)

        #get the last timestamp processed 
       
        last_timestamp_processed = pd.DataFrame(list(session.execute(f'SELECT max(LPRD) as last_record FROM "statistics_1";')))['last_record'][0]
        
        new_data = pd.DataFrame(list(session.execute(f"SELECT * FROM Event where \
            timestamp >= '{last_timestamp_processed}' allow filtering;")))

        if(new_data.shape[0] > 0): #if there is new data

            for i in range(new_data.shape[0]):

                #get the timestamp of that data

                record_ts = new_data.loc[i,"timestamp"]

                #check if record_ts has been already in statistics_1

                date_1 = (datetime.strptime(str(record_ts), "%Y-%m-%d"))
                end_date = str(date_1.date())

                ts_in_stat1 = pd.DataFrame(list(session.execute(f'SELECT day FROM "statistics_1" where day={end_date} allow filtering;')))['day'][0]

                if(ts_in_stat1!=None):
                    #day exists in statistics_1
                    query =  session.prepare('SELECT type from Card where id_client = ? ALLOW FILTERING;')

            
                    card_type = pd.DataFrame(list(session.execute(query,[new_data.loc[i,"id_user"]])))['type'][0]

                    if(card_type == "month"):

                        NS=int(pd.DataFrame(list(session.execute(f'SELECT new_subs FROM "statistics_1 where day={end_date}";')))['new_subs'][0]) + 1
                        MU=int(pd.DataFrame(list(session.execute(f'SELECT month_user FROM "statistics_1 where day={end_date}";')))['month_user'][0]) + 1
                        session.execute(f"UPDATE statistics_1 SET \
                        month_user ='{MU}' ,new_subs ={NS},LPRD={record_ts}\
                            WHERE day = {date};")
                    else:
                        NS=int(pd.DataFrame(list(session.execute(f'SELECT new_subs FROM "statistics_1 where day={end_date}";')))['new_subs'][0]) + 1
                        YU=int(pd.DataFrame(list(session.execute(f'SELECT year_user FROM "statistics_1 where day={end_date}";')))['year_user'][0]) + 1
                        session.execute(f"UPDATE statistics_1 SET \
                        new_subs ={NS},year_user= {YU},LPRD={record_ts}\
                            WHERE day = {date};")   
                    
                else:
                    #it's a new day
                    if(card_type == "month"):

                        session.execute(f"INSERT INTO statistics_1(day, month_user, new_subs, year_user,LPRD)\
                VALUES ('{end_date}', 1,1 ,0,{record_ts});")
                    else:
                        session.execute(f"INSERT INTO statistics_1(day, month_user, new_subs, year_user,LPRD)\
                VALUES ('{end_date}', 0,1 ,1,{record_ts});")
                        
                    











        
    
def statistics_2(session):
    last_day = df = pd.DataFrame(list(session.execute('SELECT max(day) as last_day FROM "statistics_2";')))['last_day'][0]
    if last_day != None:
        #get the users for the new day
        date_1 = datetime.strptime(str(last_day), "%Y-%m-%d")
        end_date = date_1 + timedelta(days=1)
        #end_date = time.mktime(datetime.datetime.strptime(end_date, "%Y/%m/%d").timetuple())

        try:

            dates = pd.DataFrame(list(session.execute(f"SELECT toDate(timestamp) as date\
            FROM Event where timestamp >= '{end_date}' allow filtering;")))['date'].unique()
        except:
            print("No new data to process !!")
            return
        
    else:
        dates = pd.DataFrame(list(session.execute(f'SELECT toDate(timestamp) as date\
         FROM Event;')))['date'].unique()
    #print(dates)
    dates.sort()
    for date in dates:
        date_time_obj = datetime. strptime(str(date), '%Y-%m-%d')
        if (date_time_obj.date() >= datetime.now().date()):
                print(f"you have to wait until next day to process {date_time_obj.date()} data")
                return
        date_1 = (datetime.strptime(str(date), "%Y-%m-%d"))
        end_date = date_1 + timedelta(days=1)
        end_date = str(end_date.date())
        date = str(date)
        #""" date_1 = datetime.datetime.strptime(date, "%Y-%m-%d")
        #end_date = date_1 + timedelta(days=1)
        #end_date = time.mktime(datetime.datetime.strptime(end_date, "%Y-%m-%d").timetuple())

        #start =  time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()) """
    
        data = pd.DataFrame(list(session.execute(f"SELECT id_event, end_station, id_card ,\
         id_user, line, start_station, timestamp, toDate(timestamp) as day FROM Event\
          where timestamp >= '{date}' and timestamp < '{end_date}' allow filtering;")))

        for i in range(data.shape[0]):
            date = data.loc[i,'timestamp']
            d = str(date.time().hour)
            if( d == '0'):
                data.loc[i,'Hour'] = "00"
            else:
                data.loc[i,'Hour'] = d
            


        #get start_hour
        hours = ["00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"]
        lines = ['A','B','C']
        for index in range(len(hours)):
                    start_hour = hours[index]
                    try:
                        end_hour = hours[index+1]
                    
                    except:
                        end_hour = "00"
                    #print(f"{start_hour} ---> {end_hour}")
                    df = data[(data['day'] == date) & (data['Hour']==start_hour)]

                    tram_lines = {}

                    for line in lines:
                        tram_lines[line] = {}
                        data_for_line = df[df['line'] == line]

                        tram_lines[line]['users'] = data_for_line.shape[0]

                        for station in data_for_line['start_station'].unique():
                            #print(station)

                            data_for_station = data_for_line[data_for_line['start_station'] == station]

                            tram_lines[line][station] = data_for_station.shape[0]
                    #print(tram_lines)
                    
        
        #write result in cassandra
                    #print("ok")
                    max_id = pd.DataFrame(list(session.execute(f'SELECT max(id) as max\
         FROM "statistics_2";')))[max].unique()[0]
                    if(max_id == None):
                        max_id=1
                    else:
                        max_id = max_id+1

                    session.execute(f"INSERT INTO statistics_2(id, day, end_hour, start_hour,tram_A, tram_B, tram_C)\
VALUES ({max_id},'{date.date()}','{start_hour}','{end_hour}',{tram_lines['A']} , {tram_lines['B']},{tram_lines['C']});")

        print("statistics are saved!!")


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
                if(card_type == "month"):
                    details['month'].append(user)
                elif(card_type == "year"):
                    details['year'].append(user)

            if(update==0):
                session.execute(f"INSERT INTO statistics_1(day, month_user, new_subs, year_user,LPRD)\
                VALUES ('{date}', {len(details['month'])},{len(new_users)} ,{len(details['year'])},{users_data.loc[(users_data.shape[0]-1),'timestamp']});")
                
            else:
                NS=int(pd.DataFrame(list(session.execute(f'SELECT new_subs FROM "statistics_1 where day={date}";')))['new_subs'][0]) + len(new_users)
                MU=int(pd.DataFrame(list(session.execute(f'SELECT month_user FROM "statistics_1 where day={date}";')))['month_user'][0]) + len(details['month'])
                YU=NS-MU
                session.execute(f"UPDATE statistics_1 SET \
                    month_user ='{MU}' ,new_subs ={NS},year_user= {YU},LPRD={users_data.loc[(users_data.shape[0]-1),'timestamp']}\
                        WHERE day = {date};")
            
            print("statisctics saved!")


def top_line_user(session):

    #take all users
    clients = pd.DataFrame(list(session.execute(f"SELECT * FROM Client;")))

    #for each user get his last index in the event table
    for i in range(clients.shape[0]):

        last_index_Event = pd.DataFrame(list(session.execute(f"SELECT max(id_event) as max FROM Event where \
        id_user = {clients.loc[i,'id_client']} ALLOW FILTERING;")))['max'].unique()
        if(last_index_Event[0] != None): #if the user have already used the train
            if(math.isnan(clients.loc[i,'last_index'])):
                clients.loc[i,'last_index'] = -1

            if(clients.loc[i,'last_index'] < last_index_Event):  #if there is new data for the current user then update data        
                    #print(clients.loc[i,'id_client'])
                    client = pd.DataFrame(list(session.execute(f"SELECT * FROM Event where \
                                id_user = {clients.loc[i,'id_client']} ALLOW FILTERING;")))
                    #print(client)
                    most_used_line =list(client.groupby('line').count().reset_index().sort_values(
                        by=['id_card'], ascending=False).head(1)['line'])[0]
                    #print(clients.loc[i,'id_client'])
                    #Update value
                    session.execute(f"UPDATE Client SET \
                    top_line ='{most_used_line}' ,last_index ={last_index_Event[0]} WHERE id_client = {int(clients.loc[i,'id_client'])};")

                    print(f"top used line for client {clients.loc[i,'id_client']} is updated :)")
                

if __name__ == "__main__":

    print("generating type 1 statictics.........")

    statistics_1(session)

    print("generating type 2 statictics.........")
    statistics_2(session)

    print("updating top users who have used tram lines.........")
    top_line_user(session)



