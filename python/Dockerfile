FROM python:3.8

RUN pip install cassandra-driver
RUN pip install pandas


WORKDIR /usr/app/src

COPY python_app.py ./
RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "vim"]

#CMD ["python", "-u", "python_app3.py"]

