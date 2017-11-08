# 
# MIT License
#
# Copyright (c) 2017 Saniya Tech Inc.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
# associated documentation files (the "Software"), to deal in the Software without restriction, 
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
# LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# 
# 


FROM openjdk:8-jdk-alpine

MAINTAINER Saniya Tech <saniyatech@gmail.com>

##### Spark Environment Vars #####
ENV INSTALL_DIR /usr/local
ENV SPARK_HOME $INSTALL_DIR/spark
ENV SPARK_VERSION 2.2.0
ENV HADOOP_VERSION 2.7
ENV SPARK_TGZ_URL https://www.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

##### Update apk and install bash and curl #####
RUN apk update \
      && apk upgrade \
      && apk add --update bash \
      && apk add --update curl \
      && rm -rf /var/cache/apk/*

##### Install Spark with HDFS #####
WORKDIR $INSTALL_DIR

RUN set -x \
      && curl -fSL "$SPARK_TGZ_URL" -o spark.tgz \
      && tar -xzf spark.tgz \
      && mv spark-* spark \
      && rm spark.tgz

##### App Environment Vars #####
ENV APP_HOME /opt/sparkapp
ENV APP_VERSION 0.0.1
ENV APP_SCALA_VERSION 2.11
ENV APP_JAR spark-app_$APP_SCALA_VERSION-$APP_VERSION.jar

##### Deploy and execute the Spark app #####
WORKDIR $APP_HOME
ADD deploy $APP_HOME
ENTRYPOINT "$SPARK_HOME/bin/spark-submit" --class org.saniyatech.spark.SparkApp --master local[2] "$APP_JAR"
