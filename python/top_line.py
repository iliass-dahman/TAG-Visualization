from cassandra.cluster import Cluster
import pandas as pd

import warnings
warnings.filterwarnings('ignore')





from datetime import datetime
from datetime import timedelta
import pandas as pd
import time


def top_line_user(session):
    
    while(True):

        time.sleep(10)

        #take all users
        clients = pd.DataFrame(list(session.execute(f"SELECT * FROM Client;")))

        #for each user get his last index in the event table
        for i in range(clients.shape[0]):

            last_index_Event = pd.DataFrame(list(session.execute(f"SELECT max(id_event) as max FROM Event where \
            id_user = '{clients.loc[i,'id_client']}' ALLOW FILTERING;")))['max'].unique()
            if(last_index_Event[0] != None): #if the user have already used the train
                if(clients.loc[i,'last_index'] == None):
                    clients.loc[i,'last_index'] = -1

                if(clients.loc[i,'last_index'] < last_index_Event):  #if there is new data for the current user then update data        
                        #print(clients.loc[i,'id_client'])
                        client = pd.DataFrame(list(session.execute(f"SELECT * FROM Event where \
                                    id_user = '{clients.loc[i,'id_client']}' ALLOW FILTERING;")))
                        #print(client)
                        most_used_line =list(client.groupby('line').count().reset_index().sort_values(
                            by=['id_card'], ascending=False).head(1)['line'])[0]
                        #print(clients.loc[i,'id_client'])
                        #Update value
                        session.execute(f"UPDATE client SET \
                        top_line ='{most_used_line}' ,last_index ={last_index_Event[0]} WHERE id_client = '{clients.loc[i,'id_client']}';")

                        print(f"top used line for client {clients.loc[i,'id_client']} is updated :)")






if __name__ == "__main__":
    
    print("Connecting to the Cluster..........")
    #connect to the cluster
    cluster = Cluster(["docker-cassandra-1"],port=9042)


    #connect to the keyspace
    session = cluster.connect('test')

    print("generating top line statistics.........")

    top_line_user(session)
