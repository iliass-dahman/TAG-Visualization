FROM python:3.8

RUN pip install kafka
RUN pip install kafka-python


WORKDIR /usr/app/src

COPY *.* ./

CMD python3 consumer.py

