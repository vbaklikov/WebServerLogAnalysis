#!/usr/bin/env bash

export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

bin/spark-submit --class com.klyk.webserverloganalysis.BasicLogAnalysis --jars ../../ScalaWorkspace/ApacheCLFParser/target/scala-2.11/apacheclfparser_2.11-1.0.jar --master local[4] ~klyk/IdeaProjects/WebServerLogAnalysis/target/scala-2.11/webserverloganalysis_2.11-0.1.0-SNAPSHOT.jar local ../../../Downloads/usb/data/nasaweblogs/access_log_Jul95 ../../../Downloads/usb/data/nasaweblogs/output.txt