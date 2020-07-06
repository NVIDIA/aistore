FROM python:3.8.3-alpine3.11

RUN mkdir /code
WORKDIR /code
COPY server.py server.py

EXPOSE 80

ENTRYPOINT [ "/code/server.py", "--listen", "0.0.0.0", "--port", "80" ]
