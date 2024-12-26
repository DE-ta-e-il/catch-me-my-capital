# Apache Airflow의 기본 이미지 사용
FROM apache/airflow:2.10.1

# 작업 디렉토리 설정
WORKDIR /opt/airflow

# requirements.txt 파일 복사
COPY requirements.txt .

# Python 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt
