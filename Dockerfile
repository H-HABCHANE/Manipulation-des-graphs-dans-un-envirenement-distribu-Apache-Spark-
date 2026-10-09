FROM apache/spark:3.5.3-scala2.12-java17-ubuntu
USER root
ARG LOCAL_UID=1000
ARG LOCAL_GID=1000
RUN groupadd --non-unique --gid "$LOCAL_GID" graphapp \
    && useradd --non-unique --uid "$LOCAL_UID" --gid graphapp --create-home graphapp
WORKDIR /app
COPY src /app/src
COPY scripts /app/scripts
RUN bash scripts/compile.sh
COPY data /app/data
COPY docs /app/docs
ENV SPARK_LOCAL_IP=127.0.0.1
USER graphapp
ENTRYPOINT ["bash", "/app/scripts/run-local.sh"]
CMD ["analyze", "data/demo", "results/demo"]
