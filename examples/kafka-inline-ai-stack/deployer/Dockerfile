# Tiny SDK app that deploys the pipeline, then exits. Pulls the published Pulse
# Java SDK from Maven Central (no engine source needed). Context = the example
# dir (see docker-compose.yml).
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /b
COPY deployer/pom.xml ./pom.xml
COPY Deployer.java ./src/main/java/Deployer.java
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /b/target/deployer.jar /app/deployer.jar
COPY wasm-fraud-features/fraud-features.wasm /app/fraud-features.wasm
COPY model/fraud-risk.onnx /app/fraud-risk.onnx
ENV WASM_PATH=/app/fraud-features.wasm
ENV MODEL_PATH=/app/fraud-risk.onnx
ENTRYPOINT ["java", "-jar", "/app/deployer.jar"]
