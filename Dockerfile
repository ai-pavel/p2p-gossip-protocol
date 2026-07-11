FROM ziglang/zig:0.13.0 AS build
WORKDIR /app
COPY build.zig src/ ./
RUN zig build -Doptimize=ReleaseFast

FROM alpine:3.19
COPY --from=build /app/zig-out/bin/gossip /usr/local/bin/gossip
EXPOSE 7946/udp
ENTRYPOINT ["/usr/local/bin/gossip"]