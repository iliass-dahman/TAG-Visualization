from cassandra.cluster import Cluster
import pandas as pd
import math
import warnings
warnings.filterwarnings('ignore')


#connect to the cluster
cluster = Cluster(['docker-cassandra-1'],port=9042)


#connect to the keyspace
session = cluster.connect('test')



from datetime import datetime
from datetime import timedelta
import pandas as pd
import time


def statistics_1(session):
    last_day = pd.DataFrame(list(session.execute('SELECT max(day) as last_day FROM "statistics_1";')))['last_day'][0]
    #print(last_day)
    if last_day != None:
        #get the users for the new day
        date_1 = datetime.strptime(str(last_day), "%Y-%m-%d")
        end_date = date_1 + timedelta(days=1)
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

            date_time_obj = datetime. strptime(str(date), '%Y-%m-%d')

            if (date_time_obj.date() >= datetime.now().date()):
                print(f"you have to wait until next day to process {date_time_obj.date()} data")
                return
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
            
            users = users['id_user'].unique()
            
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
            
            session.execute(f"INSERT INTO statistics_1(day, month_user, new_subs, year_user)\
            VALUES ('{date}', {len(details['month'])},{len(new_users)} ,{len(details['year'])} );")
            print("statisctics saved!")





        
    
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




def top_line_user(session):

    #take all users
    clients = pd.DataFrame(list(session.execute(f"SELECT * FROM Client;")))

    #for each user get his last index in the event table
    for i in range(clients.shape[0]):

        last_index_Event = pd.DataFrame(list(session.execute(f"SELECT max(id_event) as max FROM Event where \
        id_user = {clients.loc[i,'id_client']} ALLOW FILTERING;")))['max'].unique()
        print(last_index_Event)
        if(last_index_Event != None): #if the user have already used the train
            if(clients.loc[i,'last_index'] == None):
                clients.loc[i,'last_index'] = -1

            if(clients.loc[i,'last_index'] < last_index_Event):  #if there is new data for the current user then update data
                    print(clients.loc[i,'id_client'])
                    client = pd.DataFrame(list(session.execute(f"SELECT * FROM Event where \
                                id_user = {clients.loc[i,'id_client']} ALLOW FILTERING;")))
                    #print(client)
                    most_used_line =list(client.groupby('line').count().reset_index().sort_values(
                        by=['id_card'], ascending=False).head(1)['line'])[0]
                    print(clients.loc[i,'id_client'])
                    #Update value
                    session.execute(f"UPDATE \"Client\" SET \
                    top_line ='{most_used_line}' ,last_index ={last_index_Event[0]} WHERE id_client = {int(clients.loc[i,'id_client'])};")
                    
                    print(f"top used line for client {clients.loc[i,'id_client']} is updated :)")

                



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
