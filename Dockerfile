FROM alpine:3.19

# Automatically set by Docker when building for multiple platforms
ARG TARGETARCH

# Install runtime dependencies
RUN apk add --no-cache \
    rsync \
    openssh-client \
    bash \
    ca-certificates

# Copy pre-built malai binary for the target architecture
COPY binaries/${TARGETARCH}/malai /usr/local/bin/malai
RUN chmod +x /usr/local/bin/malai

# Copy entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Create data directory
RUN mkdir -p /data

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["help"]
