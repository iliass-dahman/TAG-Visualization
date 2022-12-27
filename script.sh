#!/usr/bin/env bash

until printf "" 2>>/dev/null >>/dev/tcp/cassandra/9042; do 
    sleep 5;
    echo "Waiting for cassandra...";
done

echo "Creating keyspace and table..."
cqlsh cassandra -u cassandra -p cassandra -e "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS test.Card (id_card text PRIMARY KEY,end_of_validity text,id_client int,type text);"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS test.Client (id_client text PRIMARY KEY, first_name text, last_index int, last_name text,top_line varchar);"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS test.Event (id_event int PRIMARY KEY,end_station text,id_card int,id_user int,line varchar,start_station text,timestamp timestamp);"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS test.statistics_1 (day text PRIMARY KEY,month_user int,new_subs int,year_user int);"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE TABLE IF NOT EXISTS test.statistics_2 (id int PRIMARY KEY,day text,end_hour varchar,start_hour varchar,tram_A map<text,int>,tram_B map<text,int>,tram_C map<text,int>);"

echo "Writing sample data...";
#cqlsh cassandra -u cassandra -p cassandra -e "insert into test.test (sensor_id, registered_at, temperature) values (99051fe9-6a9c-46c2-b949-38ef78858dd0, toTimestamp(now()), $(shuf -i 18-32 -n 1));";
#cqlsh cassandra -u cassandra -p cassandra -e "insert into test.test (sensor_id, registered_at, temperature) values (99051fe9-6a9c-46c2-b949-38ef78858dd1, toTimestamp(now()), $(shuf -i 12-40 -n 1));";

#cqlsh cassandra -u cassandra -p cassandra -e "select * from test.test;"
#ls /var/lib/
#ls /var/lib/cassandra/
#for i in /home/folder1/*.csv ; do 
#    echo "COPY table FROM '$i' WITH DELIMITER=',' AND HEADER=FALSE;"|cqlsh -f -
#done

#while [ condition ];
#do
#    continue 
#done




cqlsh cassandra -u cassandra -p cassandra -e "COPY test.Card FROM '/var/lib/Card.csv' WITH DELIMITER=',' AND HEADER=TRUE;";
cqlsh cassandra -u cassandra -p cassandra -e "COPY test.Client FROM '/var/lib/Client.csv' WITH DELIMITER=',' AND HEADER=TRUE;";
cqlsh cassandra -u cassandra -p cassandra -e "COPY test.Event FROM '/var/lib/Event.csv' WITH DELIMITER=',' AND HEADER=TRUE;";