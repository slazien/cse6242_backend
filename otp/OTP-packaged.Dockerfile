#DOCKER BASE
FROM openjdk:11

#STATIC
ENV OTP_JAR=https://repo1.maven.org/maven2/org/opentripplanner/otp/1.5.0/otp-1.5.0-shaded.jar
ENV BASE_PATH=otp
ENV graphs=https://aurimasazure.blob.core.windows.net/cse6242/graphs.tar.gz

RUN apt update && apt install -y wget tar gzip

#DOWNLOAD OTP & graph data
RUN mkdir $BASE_PATH && \ 
    wget $OTP_JAR -O otp.jar && \ 
    wget $graphs -O graphs.tar.gz 

#unzip the graphs
RUN tar -xvf graphs.tar.gz -C $BASE_PATH/

#SERVE
CMD java -Xmx5G -jar ./otp.jar --basePath otp/ --server --port 8062 --securePort 8802 \
--router atlanta --router dallas --router new_york --router los_angeles --router chicago

EXPOSE 8062
EXPOSE 8802
