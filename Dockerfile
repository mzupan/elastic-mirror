# Stage 1: Build the plugin
FROM gradle:7.6.4-jdk11 AS builder

WORKDIR /build
COPY build.gradle settings.gradle ./
COPY gradle/ gradle/
COPY gradlew ./
COPY src/ src/
COPY plugin-security.policy ./

RUN gradle pluginZip --no-daemon

# Stage 2: ES image with plugin installed
FROM docker.elastic.co/elasticsearch/elasticsearch:7.17.25

# Copy the plugin zip from builder
COPY --from=builder /build/build/distributions/elastic-mirror-1.0.0.zip /tmp/plugin.zip

# Install the plugin (--batch skips confirmation prompt)
RUN elasticsearch-plugin install --batch file:///tmp/plugin.zip && \
    rm /tmp/plugin.zip
