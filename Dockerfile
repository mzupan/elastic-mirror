# Stage 1: Build the plugin
FROM gradle:7.6.4-jdk17 AS builder

ARG PLUGIN_VERSION=1.0.0-dev

WORKDIR /build
COPY build.gradle settings.gradle ./
COPY gradle/ gradle/
COPY gradlew ./
COPY src/ src/
COPY plugin-security.policy ./

RUN gradle pluginZip --no-daemon -PpluginVersion=${PLUGIN_VERSION}

# Stage 2: ES image with plugin installed
FROM docker.elastic.co/elasticsearch/elasticsearch:8.14.3

# Copy the plugin zip from builder (wildcard since version varies)
COPY --from=builder /build/build/distributions/elastic-mirror-*.zip /tmp/plugin.zip

# Install the plugin (--batch skips confirmation prompt)
RUN elasticsearch-plugin install --batch file:///tmp/plugin.zip; \
    rm -f /tmp/plugin.zip 2>/dev/null || true
