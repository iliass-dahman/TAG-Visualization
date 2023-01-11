FROM python:3.8

RUN pip install cassandra-driver
RUN pip install pandas
RUN pip install kafka-python


WORKDIR /usr/app/src

COPY *.* ./

# add execution permission to ./run_producer.sh
RUN chmod +x ./run_producer.sh

RUN pip install -r requirements.txt

RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "vim"]
#CMD ["sleep 90s"]
#CMD ["python3", "./producer.py", "&"]
#CMD ["python3", "./consumer.py", "&"]
#CMD ["sleep 40s"]
#CMD ["python3", "./stat1.py", "&"]
#CMD ["python3", "./stat2.py", "&"]
#CMD ["python3", "./top_line.py", "&"]

#CMD ["python", "-u", "python_app3.py"]

