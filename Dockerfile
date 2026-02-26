# Stage 1: Build common library
FROM maven:3.9-eclipse-temurin-17 AS common-build
RUN git clone https://github.com/netflexity/netflexity-anypoint-common.git /common
WORKDIR /common
RUN mvn clean install -DskipTests

# Stage 2: Build exporter
FROM maven:3.9-eclipse-temurin-17 AS build
COPY --from=common-build /root/.m2 /root/.m2
COPY . /app
WORKDIR /app
RUN mvn clean package -DskipTests

# Stage 3: Runtime
FROM eclipse-temurin:17-jre
COPY --from=build /app/target/*.jar /app/app.jar
EXPOSE 9101
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
