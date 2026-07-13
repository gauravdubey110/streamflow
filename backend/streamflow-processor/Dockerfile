# ─────────────────────────────────────────────────────────────────────────────
# streamflow-processor — multi-stage Dockerfile
# SPEC-21 R2: JRE base eclipse-temurin:17-jre-alpine; layered JAR extraction.
#
# Build from repo root:
#   docker build -f backend/streamflow-processor/Dockerfile \
#                -t streamflow-processor:latest .
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: build ───────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy parent POM and all module POMs first (layer-cache friendly)
COPY backend/pom.xml ./backend/pom.xml
COPY backend/streamflow-common/pom.xml ./backend/streamflow-common/pom.xml
COPY backend/streamflow-producer/pom.xml ./backend/streamflow-producer/pom.xml
COPY backend/streamflow-processor/pom.xml ./backend/streamflow-processor/pom.xml
COPY backend/streamflow-api/pom.xml ./backend/streamflow-api/pom.xml

# Resolve all dependencies (cached layer; only re-runs when POMs change)
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -f backend/pom.xml dependency:go-offline -DskipTests

# Copy source code for common + processor only
COPY backend/streamflow-common/src ./backend/streamflow-common/src
COPY backend/streamflow-processor/src ./backend/streamflow-processor/src

# Build: install common, then package processor (skip tests — tests run in CI separately)
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -f backend/pom.xml install \
        -pl streamflow-common,streamflow-processor \
        -am -DskipTests

# Extract layered JAR using Spring Boot layertools
WORKDIR /build/extracted
RUN java -Djarmode=layertools \
    -jar /build/backend/streamflow-processor/target/streamflow-processor-0.0.1-SNAPSHOT.jar \
    extract

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine AS runtime

# Non-root user for security
RUN addgroup -S streamflow && adduser -S streamflow -G streamflow
USER streamflow

WORKDIR /app

# Copy extracted layers in dependency-stability order (least → most volatile)
COPY --from=builder /build/extracted/dependencies/ ./
COPY --from=builder /build/extracted/spring-boot-loader/ ./
COPY --from=builder /build/extracted/snapshot-dependencies/ ./
COPY --from=builder /build/extracted/application/ ./

EXPOSE 8082

# JVM flags: container-aware heap sizing; prefer IPv4
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.net.preferIPv4Stack=true", \
  "org.springframework.boot.loader.launch.JarLauncher"]
