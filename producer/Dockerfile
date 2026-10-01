FROM gradle:8.14.3-jdk21 AS build
WORKDIR /build
COPY build.gradle settings.gradle ./
COPY src ./src
RUN gradle --no-daemon installDist

FROM eclipse-temurin:21-jre
WORKDIR /lab
COPY --from=build /build/build/install/log-producer /app
ENTRYPOINT ["/app/bin/log-producer"]
