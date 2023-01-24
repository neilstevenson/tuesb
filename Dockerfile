FROM quay.io/centos/s390x:stream9

WORKDIR /build

RUN echo Version 6

RUN echo arch
RUN arch
RUN echo yum -y install java
RUN yum -y install java
RUN echo java --version
RUN java --version
RUN echo yum -y install maven
RUN yum -y install maven
RUN echo mvn -version
RUN mvn -version

COPY pom.xml .
COPY src src
RUN mvn package

ENV MY_HAZELCAST_LICENSE=${MY_HAZELCAST_LICENSE}    
ENV JAVA_ARGS ""
ENV JAVA_OPTS "--add-modules java.se --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
    --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED \
    --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"

CMD ["bash", "-c", "set -euo pipefail \
      && echo @@@@@@@@@@ \
      && echo arch \
      && arch \
      && echo @@@@@@@@@@ \
      && echo java --version \
      && java --version \
      && echo @@@@@@@@@@ \
      && echo java $JAVA_ARGS $JAVA_OPTS \
 -Dhazelcast.enterprise.license.key=${MY_HAZELCAST_LICENSE} \
 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener \
 -jar /build/target/tuesb.jar \
      && java $JAVA_ARGS $JAVA_OPTS \
 -Dhazelcast.enterprise.license.key=${MY_HAZELCAST_LICENSE} \
 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener \
 -jar /build/target/tuesb.jar \
     "] 
