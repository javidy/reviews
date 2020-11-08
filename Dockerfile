FROM puckel/docker-airflow:1.10.9

USER root

RUN update-ca-certificates -f \
  && apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y \
    wget \
    git \
    libatlas3-base \
    libopenblas-base \
  && apt-get clean
  
RUN echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list                                                   
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk

RUN apt-get install unzip -y && \
    apt-get autoremove -y

# SPARK
RUN cd /usr/ \
    && wget "http://apache.mirrors.spacedump.net/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz" \
    && tar xzf spark-3.0.1-bin-hadoop3.2.tgz \
    && rm spark-3.0.1-bin-hadoop3.2.tgz \
    && mv spark-3.0.1-bin-hadoop3.2 spark

ENV SPARK_HOME /usr/spark
ENV PATH="/usr/spark/bin:${PATH}"
ENV SPARK_MAJOR_VERSION 3
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$SPARK_HOME/python/:$PYTHONPATH

RUN mkdir -p /usr/spark/work/ \
    && chmod -R 777 /usr/spark/work/

ENV SPARK_MASTER_PORT 7077

USER airflow