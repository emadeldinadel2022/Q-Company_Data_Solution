FROM alpine:latest

RUN apk add --no-cache python3 py3-pip bash py3-pyftpdlib

RUN mkdir -p /var/ftp/q_company_data

EXPOSE 21 60000-60010

# Copy the dataset into the container
COPY ../dataset /var/ftp/q_company_data

COPY ftp_server/ftp_server.py /ftp_server.py
COPY ftp_server/ftp_client.py /ftp_client.py

CMD ["python3", "/ftp_server.py"]