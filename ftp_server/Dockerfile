FROM alpine:latest

RUN apk add --no-cache python3 py3-pip bash py3-pyftpdlib

RUN mkdir -p /var/ftp/q_company_data/group1 
RUN mkdir -p /var/ftp/q_company_data/group2 
RUN mkdir -p /var/ftp/q_company_data/group3 
RUN mkdir -p /var/ftp/q_company_data/group4
RUN mkdir -p /var/ftp/q_company_data/group5 
RUN mkdir -p /var/ftp/q_company_data/group6 

EXPOSE 21 60000-60010

COPY ftp_server/ftp_server.py /ftp_server.py

CMD ["python3", "/ftp_server.py"]