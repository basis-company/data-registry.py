FROM python:3.11-bullseye

WORKDIR /app

COPY ./requirements.txt /app
RUN pip install --upgrade pip --no-cache-dir -r requirements.txt

COPY ./registry /app/registry
COPY ./tests /app/tests

CMD pytest -s
