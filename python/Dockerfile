FROM python:3.8

RUN pip install cassandra-driver
RUN pip install pandas
RUN pip install -r requirements.txt


WORKDIR /usr/app/src

COPY *.* ./


RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "vim"]

#CMD ["python", "-u", "python_app3.py"]

