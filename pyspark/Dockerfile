FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
ARG LOCAL_UID=1000
ARG LOCAL_GID=1000
RUN groupadd --non-unique --gid "$LOCAL_GID" graphapp \
    && useradd --non-unique --uid "$LOCAL_UID" --gid graphapp --create-home graphapp
# PySpark est livré dans l'image sous forme d'archives : les rendre importables.
ENV PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/app/src \
    PYSPARK_PYTHON=python3 PYTHONDONTWRITEBYTECODE=1 SPARK_LOCAL_IP=127.0.0.1
WORKDIR /app
COPY src /app/src
COPY tests /app/tests
COPY scripts/run.sh /app/scripts/run.sh
USER graphapp
ENTRYPOINT ["bash", "/app/scripts/run.sh"]
CMD ["analyze", "data/demo", "results/demo"]
