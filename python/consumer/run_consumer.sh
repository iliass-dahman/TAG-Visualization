sleep 120s 
python3 ./producer.py &
python3 ./consumer.py &
sleep 30s 
python3 ./stat1.py &
python3 ./stat2.py &
python3 ./top_line.py 