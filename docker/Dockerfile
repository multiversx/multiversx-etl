FROM python:3.10-slim

ARG USERNAME=multiversx
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create the user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt && rm /requirements.txt

# Switch to regular user
USER $USERNAME
WORKDIR /home/${USERNAME}/app

COPY ./multiversxetl /home/${USERNAME}/app/multiversxetl
COPY ./schema /home/${USERNAME}/app/schema

ENV PYTHONPATH=/home/${USERNAME}/app
