FROM bitnami/spark:3

# Dependências Python
COPY requirements.txt .
RUN pip install -r requirements.txt

# Conectores JDBC
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar /opt/bitnami/spark/jars/

# Código da aplicação
COPY etl_spark.py /app/
COPY config_bigdata.json /app/
COPY mapping.json /app/

WORKDIR /app
ENTRYPOINT ["spark-submit", "--master", "local[*]", "etl_spark.py"]
CMD ["config_bigdata.json"]
