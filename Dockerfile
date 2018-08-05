FROM python:3

RUN pip install pandas requests

ADD cryptoscrap /app/cryptoscrap
ADD app.py /app/

ENTRYPOINT ["python", "/app/app.py"]
