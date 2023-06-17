FROM openjdk:19
MAINTAINER KulikovPavel
COPY build/libs/JavaServiceDeduplication-1.0-SNAPSHOT.jar Deduplication.jar
ENTRYPOINT ["java","-jar","/Deduplication.jar"]