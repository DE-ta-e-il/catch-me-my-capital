FROM apache/airflow:2.10.4

COPY requirements.txt /requirements.txt

# this is prolly why we should use a separate driver image
# welp as long as the mwaa don't charge on compute resource basis... :P :P :P :P

USER root

RUN apt update \
    && apt install -y libnss3 libx11-xcb1 libxcomposite1 libxcursor1 libxdamage1 libxrandr2 libxi6 libxtst6 fonts-liberation libappindicator3-1 libasound2 \
    && apt install wget \
    && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt install -y ./google-chrome-stable_current_amd64.deb \
    && rm -rf ./google-chrome-stable_current_amd64.deb

USER airflow

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt
