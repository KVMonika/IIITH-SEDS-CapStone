FROM python:3.8-slim
WORKDIR /app

ENV PYTHONUNBUFFERED 1
ENV DISPLAY=:99

# ENV PYSPARK_DRIVER_PYTHON python3
# ENV PYSPARK_PYTHON python3

RUN mkdir -p /usr/share/man/man1

RUN apt update
RUN apt install default-jdk scala git -y

# Install requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /app
#ENTRYPOINT python3 main.py
