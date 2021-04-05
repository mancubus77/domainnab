#FROM python:3.8-alpine
FROM registry.redhat.io/rhel8/python-38
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip \
    && pip install -r requirements.txt
#    && apk update \
#    && apk add postgresql-dev gcc python3-dev musl-dev
COPY . .
CMD [ "python3", "main.py"]
