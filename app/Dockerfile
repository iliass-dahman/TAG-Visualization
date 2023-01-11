FROM python:3.8


WORKDIR /usr/app/src

# Copy every file and folder to ./ in the container
COPY . .

RUN pip install -r requirements.txt

RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "vim"]