# Install Zig directly instead of pulling ghcr.io/ziglang/zig (ghcr anonymous
# pulls return 403 in this environment); arch-aware for amd64/arm64 builds.
FROM alpine:3.19 AS build
RUN apk add --no-cache curl xz
RUN ARCH=$(uname -m) && \
    curl -fsSL "https://ziglang.org/download/0.13.0/zig-linux-${ARCH}-0.13.0.tar.xz" \
      | tar -xJ -C /opt && \
    ln -s "/opt/zig-linux-${ARCH}-0.13.0/zig" /usr/local/bin/zig
WORKDIR /app
COPY build.zig ./
COPY src/ src/
RUN zig build -Doptimize=ReleaseFast

FROM alpine:3.19
COPY --from=build /app/zig-out/bin/gossip /usr/local/bin/gossip
EXPOSE 7946/udp
EXPOSE 8080/tcp
ENTRYPOINT ["/usr/local/bin/gossip"]