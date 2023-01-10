from cassandra.cluster import Cluster
import pandas as pd
import math
import warnings
warnings.filterwarnings('ignore')


import json
from datetime import datetime
from datetime import timedelta
import pandas as pd
import time
from kafka import KafkaProducer




def update(data,index,A,B,C):

    if(data.loc[index,"line"]== "A"):

        A["users"] = A["users"] + 1

        if(data.loc[index,"start_station"] in A):
            A[data.loc[index,"start_station"]] = A[data.loc[index,"start_station"]] + 1
        else:
            A[data.loc[index,"start_station"]] = 1

    elif(data.loc[index,"line"]== "B"):
            B["users"] = B["users"] + 1
            if(data.loc[index,"start_station"] in B):
                B[data.loc[index,"start_station"]] = B[data.loc[index,"start_station"]] + 1
            else:
                B[data.loc[index,"start_station"]] = 1                       
    else:       

            C["users"] = C["users"] + 1
            if(data.loc[index,"start_station"] in C):
                C[data.loc[index,"start_station"]] = C[data.loc[index,"start_station"]] + 1
            else:
                C[data.loc[index,"start_station"]] = 1







def StreamingStat2(session,producer):
    from_to = {"00":"01","01":"02","02":"03","03":"04","04":"05","05":"06","06":"07","07":"08","08":"09","09":"10","10":"11",
    "11":"12","12":"13","13":"14","14":"15","15":"16","16":"17","17":"18","18":"19","19":"20","20":"21","21":"22","22":"23","23":"00"}
    #Process Remaining data
    #max_day = get the max day in stat2
    last_day = pd.DataFrame(list(session.execute('SELECT max(day) as last_day FROM "statistics_2";')))['last_day'][0]

    if last_day != None: #stat2 is not empty
        last_day = last_day.split(" ")[0]
        #get the the timestamp for the last processed record
        last_record_ts = pd.DataFrame(list(session.execute(f'SELECT max(LPRD) as last_record FROM "statistics_2" where \
            day=\'{last_day}\' allow filtering;')))['last_record'][0]
        #get the day that follow the current date 
        date_1 = datetime.strptime(last_day, "%Y-%m-%d")
        end_date = date_1 + timedelta(days=1)
        #get the data ordrer by timestamp
        data = pd.DataFrame(list(session.execute(f"SELECT * FROM Event where \
            timestamp > '{last_record_ts}' and  timestamp < '{end_date}' allow filtering;")))

        

        #for i in range(data.shape[0]):
        #    print(data.loc[i,'timestamp'])
        
        if(data.shape[0]>0):#if there is remaining data
            #get the hour of transaction for the first record
            data.sort_values(by='timestamp', ascending=True,ignore_index=True, inplace=True)

            data['from'] = data['timestamp'].apply(lambda x: (str(x).split(" ")[1]).split(":")[0])

            #get the first start_hour
            from_0 = data.loc[0,"from"]
            
            i=0 # to iterate through the data
            #check if the start_hour exists in statistics_2

            check_query = pd.DataFrame(list(session.execute(f"SELECT id FROM statistics_2 where day='{last_day}' and from_='{from_0}' allow filtering;")))

            print("Processing Remaining data............")

            if(check_query.shape[0] > 0):

                

                new_hour = False

                old_stats = pd.DataFrame(list(session.execute(f"SELECT id, tram_A,tram_B,tram_C FROM statistics_2 where day='{last_day}' and from_ ='{from_0}' allow filtering;")))

                #print(old_stats)

                line_A = old_stats.loc[0,"tram_a"]
                line_B = old_stats.loc[0,"tram_b"]
                line_C = old_stats.loc[0,"tram_c"]
                
                while(i < data.shape[0]):

                    sh = data.loc[i,"from"]
                    
                    if (sh != from_0):
                        
                        new_hour = True

                        session.execute(f"UPDATE statistics_2 SET \
                        tram_A ={line_A},tram_B= {line_B},tram_C={line_C},LPRD='{data.loc[i,'timestamp']}'\
                            WHERE id = {old_stats.loc[0,'id']};")
                        
                        to_send = {"day": last_day, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                        producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8'))

                        del line_A
                        del line_B
                        del line_C

                        break
                    
                    #updating statistics
                    update(data,i,line_A,line_B,line_C)                      

                    if(i== (data.shape[0] - 1)):
                        
                        session.execute(f"UPDATE statistics_2 SET \
                        tram_A ={line_A},tram_B= {line_B},tram_C={line_C},LPRD='{data.loc[i,'timestamp']}'\
                            WHERE id = {old_stats.loc[0,'id']};")

                        to_send = {"day": last_day, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                        producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8'))      

                        del line_A
                        del line_B
                        del line_C

                    i+=1

                j = old_stats.loc[0,'id'] + 1

                if(new_hour):


                    
                    from_0 = data.loc[i,"from"]



                    line_A = {"users":0}
                    line_B = {"users":0}
                    line_C = {"users":0}

                    while(i < data.shape[0]):
                        
                        if(from_0 == data.loc[i,"from"]):

                                update(data,i,line_A,line_B,line_C)
                                
                                if(i== (data.shape[0]-1)):

                                    session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
            VALUES ({j}, '{last_day}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{data.loc[i,'timestamp']}');")

                                    to_send = {"day": last_day, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                                    producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8'))     

                                    j+=1 
                            
                                                     
                                i+=1
                        
                        else:
                            

                            session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
            VALUES ({j}, '{last_day}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{data.loc[i-1,'timestamp']}');")


                            to_send = {"day": last_day, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                            producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8'))     
                            
                            j+=1 

                            del line_A
                            del line_B
                            del line_C

                            line_A = {"users":0}
                            line_B = {"users":0}
                            line_C = {"users":0}


                            from_0 = data.loc[i,'from']
            else:#it's a new hour




                    from_0 = data.loc[i,"from"]

                    previous_hour = list(from_to.keys())[list(from_to.values()).index(from_0)]
                    
                    #get the index of there previous record of the hour before the current hour

                    

                    #old_id = pd.DataFrame(list(session.execute(f"SELECT id FROM statistics_2 where day='{last_day}' and from_ ='{previous_hour}' allow filtering;")))['id'][0]
                    old_id = pd.DataFrame(list(session.execute(f"SELECT max(id) as id FROM statistics_2;")))['id'][0]

                    j = old_id + 1



                    line_A = {"users":0}
                    line_B = {"users":0}
                    line_C = {"users":0}

                    while(i < data.shape[0]):
                        
                        if(from_0 == data.loc[i,"from"]):

                                update(data,i,line_A,line_B,line_C)
                                
                                if(i==data.shape[0]-1):

                                    session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
            VALUES ({j}, '{last_day}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{data.loc[i,'timestamp']}');")

                                    to_send = {"day": last_day, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                                    producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8'))     

                                    j+=1
                                                     
                                i+=1
                        
                        else:
                            session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
            VALUES ({j}, '{last_day}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{data.loc[i-1,'timestamp']}');") 
                            
                            to_send = {"day": last_day, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                            producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8'))     

                            j+=1

                            del line_A
                            del line_B
                            del line_C
                            line_A = {"users":0}
                            line_B = {"users":0}
                            line_C = {"users":0}


                            from_0 = data.loc[i,'from']

    ## Process data from the beginning

    else:

        print("Processing Data from the beginning.................")

        dates = pd.DataFrame(list(session.execute(f"SELECT toDate(timestamp) as date\
         FROM Event;")))['date'].unique()


        dates.sort()

        j=1
    
        for date in dates:
            

            date_1 = (datetime.strptime(str(date), "%Y-%m-%d"))
            end_date = date_1 + timedelta(days=1)
            end_date = str(end_date.date())
            date = str(date)
            print(f"date --> {date}")

            data =  pd.DataFrame(list(session.execute(f"SELECT * FROM Event where \
            timestamp >= '{date}' and  timestamp < '{end_date}' allow filtering;")))

            data.sort_values(by='timestamp', ascending=True,ignore_index=True, inplace=True)

            #print(data)

            #print(data.dtypes)

            #for i in range(data.shape[0]):
            #    print(data.loc[i,'timestamp'])

            data['from'] = data['timestamp'].apply(lambda x: (str(x).split(" ")[1]).split(":")[0])

            i=0

            from_0 = data.loc[i,"from"]

            line_A = {"users":0}
            line_B = {"users":0}
            line_C = {"users":0}

            

            while(i < data.shape[0]):
                        
                    if(from_0 == data.loc[i,"from"]):

                        #print(f'original {from_0}, current {data.loc[i,"from"]}')

                        update(data,i,line_A,line_B,line_C)
                                
                        if(i== (data.shape[0]-1)):
                            
                            print(f"saving data for id: {j}, date: {date}, from={from_0}")

                            session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
            VALUES ({j}, '{date}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{data.loc[i,'timestamp']}');")

                            to_send = {"day": date, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                            producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8'))     

                            j=j+1   
                                                     
                        i = i + 1
                        
                    else:
                        #print(f'changing --> original {from_0}, current {data.loc[i,"from"]}')
                        print(f"saving data for id: {j}, date: {date}, from={from_0}")
                        session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
            VALUES ({j}, '{date}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{data.loc[i-1,'timestamp']}');") 

                        to_send = {"day": date, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                        producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8')) 
                        
                        j=j+1

                        del line_A
                        del line_B
                        del line_C
                        line_A = {"users":0}
                        line_B = {"users":0}
                        line_C = {"users":0}


                        from_0 = data.loc[i,'from']


    ## Process streaming data
    print("Processing Streaming Data.........")
    while(True):


        

        time.sleep(5)

        last_timestamp_processed = pd.DataFrame(list(session.execute(f'SELECT max(LPRD) as last_record FROM "statistics_2";')))['last_record'][0]
        
        data = pd.DataFrame(list(session.execute(f"SELECT * FROM Event where \
            timestamp > '{last_timestamp_processed}' allow filtering;")))

        

        

        
        

        if(data.shape[0] > 0): #if there is new data

            data.sort_values(by='timestamp', ascending=True,ignore_index=True, inplace=True)

            data['day'] = data['timestamp'].apply(lambda x: (str(x).split(" ")[0]))

            data['from'] = data['timestamp'].apply(lambda x: (str(x).split(" ")[1]).split(":")[0])

            dates = data['day'].unique()


            dates.sort()

            #print(dates)
    
            for date in dates:
            

                tmp_data = data[data["day"] == date]
            
            

                
            
            
            
                i=0 # to iterate through the data
            #check if the start_hour exists in statistics_2

                from_0 = tmp_data.loc[0,'from']

                check_query = pd.DataFrame(list(session.execute(f"SELECT id FROM statistics_2 where day='{date}' and from_ ='{from_0}' allow filtering;")))
                
                if(check_query.shape[0] > 0):

                    new_hour = False

                    

                    old_stats = pd.DataFrame(list(session.execute(f"SELECT id, tram_A,tram_B,tram_C FROM statistics_2 where day='{date}' and from_ ='{from_0}' allow filtering;")))

                    #print(old_stats)

                    line_A = old_stats.loc[0,"tram_a"]
                    line_B = old_stats.loc[0,"tram_b"]
                    line_C = old_stats.loc[0,"tram_c"]
                    
                    while(i < tmp_data.shape[0]):

                        sh = tmp_data.loc[i,"from"]
                        record_ts = tmp_data.loc[i,"timestamp"]
                        
                        if (sh != from_0):

                            
                            new_hour = True

                            session.execute(f"UPDATE statistics_2 SET \
                            tram_A ={line_A},tram_B= {line_B},tram_C={line_C},LPRD='{tmp_data.loc[i-1,'timestamp']}'\
                                WHERE id = {old_stats.loc[0,'id']};")

                            to_send = {"day": date, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                            producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8')) 

                            del line_A
                            del line_B
                            del line_C

                            break

                        update(tmp_data,i,line_A,line_B,line_C)                      

                        if(i== (tmp_data.shape[0] - 1)):

                            print(f"new record have been processed {record_ts}")
                            
                            session.execute(f"UPDATE statistics_2 SET \
                            tram_A ={line_A},tram_B= {line_B},tram_C={line_C},LPRD='{tmp_data.loc[i,'timestamp']}'\
                                WHERE id = {old_stats.loc[0,'id']};")

                            to_send = {"day": date, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                            producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8')) 
                            
                            del line_A
                            del line_B
                            del line_C

                        i+=1

                    if(new_hour):

                        j= int(old_stats.loc[0,'id']) + 1
                        
                        from_0 = tmp_data.loc[i,"from"]
                        
                        date = str(tmp_data.loc[i,'timestamp']).split(" ")[0]

                        line_A = {"users":0}
                        line_B = {"users":0}
                        line_C = {"users":0}

                        while(i < tmp_data.shape[0]):

                            record_ts = tmp_data.loc[i,"timestamp"]

                            
                            if(from_0 == tmp_data.loc[i,"from"]):

                                    
                        
                                    print(f"new record have been processed {record_ts}")

                                    update(tmp_data,i,line_A,line_B,line_C)
                                    
                                    if(i== (tmp_data.shape[0]-1)):

                                        session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
                VALUES ({j}, '{date}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{tmp_data.loc[i,'timestamp']}');")


                                        to_send = {"day": date, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                                        producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8')) 

                                        j+=1  
                                                        
                                    i+=1
                            
                            else:

                            
                                session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
                VALUES ({j}, '{date}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{tmp_data.loc[i-1,'timestamp']}');") 
                                
                                to_send = {"day": date, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                                producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8')) 


                                j+=1



                                del line_A
                                del line_B
                                del line_C
                                line_A = {"users":0}
                                line_B = {"users":0}
                                line_C = {"users":0}


                                from_0 = tmp_data.loc[i,'from']
                else:#it's a new hour

                    
                    from_0 = tmp_data.loc[i,"from"]

                    previous_hour = list(from_to.keys())[list(from_to.values()).index(from_0)]
                    
                    #get the index of there previous record of the hour before the current hour

                    

                    old_id = pd.DataFrame(list(session.execute(f"SELECT max(id) as id FROM statistics_2;")))['id'][0]

                    j = int(old_id) + 1

                    line_A = {"users":0}
                    line_B = {"users":0}
                    line_C = {"users":0}

                    while(i < tmp_data.shape[0]):

                        record_ts = tmp_data.loc[i,"timestamp"]
                        
                        if(from_0 == tmp_data.loc[i,"from"]):

                                
                    
                                print(f"new record have been processed {record_ts}")

                                update(tmp_data,i,line_A,line_B,line_C)
                                
                                if(i==tmp_data.shape[0]-1):

                                    session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
            VALUES ({j}, '{date}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{tmp_data.loc[i,'timestamp']}');")


                                    to_send = {"day": date, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                                    producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8')) 


                                    j+=1
                                                     
                                i+=1
                        
                        else:
                            session.execute(f"INSERT INTO statistics_2(id,day,to_,from_,tram_A ,tram_B ,tram_C,LPRD )\
            VALUES ({j}, '{date}','{from_to[from_0]}','{from_0}',{line_A},{line_B},{line_C},'{tmp_data.loc[i-1,'timestamp']}');") 


                            to_send = {"day": date, "interval_start": from_0, "interval_stop": from_to[from_0], "tram_A":
                         [str(line_A)], "tram_B": [str(line_B)], "tram_C": [str(line_C)]}

                            producer.send('frequented_tram', bytes(str(to_send), encoding='utf-8')) 


                            j+=1


                            del line_A
                            del line_B
                            del line_C
                            line_A = {"users":0}
                            line_B = {"users":0}
                            line_C = {"users":0}


                            from_0 = tmp_data.loc[i,'from']        

















if __name__ == "__main__":

    print("Connecting to the Cluster..........")
    
    #connect to the cluster
    cluster = Cluster(["tram-cassandra-1"],port=9042)


    #connect to the keyspace
    session = cluster.connect('test')

    #connect to kafka 
    producer = KafkaProducer(bootstrap_servers='broker:9092')

    print("generating type 2 statictics.........")

    StreamingStat2(session,producer)