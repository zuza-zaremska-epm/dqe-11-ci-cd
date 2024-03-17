FROM jenkins/jenkins:latest
USER root
RUN mkdir /rf_tests
WORKDIR /rf_tests
RUN pwd
RUN ls -la
RUN apt-get update
RUN apt-get install -y python3-pip
RUN apt-get install python3-venv
