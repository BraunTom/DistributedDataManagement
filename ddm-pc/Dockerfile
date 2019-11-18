FROM maven:3-jdk-8-alpine as builder

# Download the dependencies from pom.xml first, in order to cache them
# (i.e. avoid re-downloading the dependencies on every code change)
# See: https://medium.com/@nieldw/caching-maven-dependencies-in-a-docker-build-dca6ca7ad612
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline

COPY src /build/src/
RUN mvn package

FROM openjdk:8-alpine
WORKDIR /root
COPY --from=builder /build/target/ddm-pc-1.0.jar .
COPY data/passwords.csv data/passwords.csv
ENTRYPOINT ["java", "-jar", "ddm-pc-1.0.jar"]
