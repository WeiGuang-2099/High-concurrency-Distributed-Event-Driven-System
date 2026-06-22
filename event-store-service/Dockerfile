# ===== Stage 1: Build with JDK 17 =====
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build

# Copy parent pom + ALL module poms so Maven can validate the reactor,
# but only copy src for the modules we actually need to build.
COPY pom.xml ./
COPY auction-common/pom.xml ./auction-common/
COPY auction-common/src ./auction-common/src
COPY gateway-service/pom.xml ./gateway-service/
COPY user-service/pom.xml ./user-service/
COPY auction-service/pom.xml ./auction-service/
COPY ticket-service/pom.xml ./ticket-service/
COPY order-service/pom.xml ./order-service/
COPY notification-service/pom.xml ./notification-service/
COPY event-store-service/pom.xml ./event-store-service/
COPY event-store-service/src ./event-store-service/src

# Install auction-common to local repo, then build ONLY event-store-service (-pl, non-recursive
# via -amd is not needed because we name the module explicitly).
RUN mvn -B -q -pl auction-common install -DskipTests \
 && mvn -B -q -pl event-store-service package -DskipTests

# ===== Stage 2: Runtime with JRE 17 =====
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /build/event-store-service/target/*.jar app.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]

