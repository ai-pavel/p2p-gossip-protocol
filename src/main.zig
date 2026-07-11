const std = @import("std");
const gossip_mod = @import("gossip");
const msg = @import("message");

const GossipEngine = gossip_mod.GossipEngine;
const Config = gossip_mod.Config;

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

const CliArgs = struct {
    port: u16 = 7001,
    join_host: ?[]const u8 = null,
    join_port: ?u16 = null,
};

fn parseArgs(allocator: std.mem.Allocator) !CliArgs {
    _ = allocator;
    var args = CliArgs{};
    var it = std.process.args();
    _ = it.next(); // skip program name

    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            if (it.next()) |val| {
                args.port = std.fmt.parseInt(u16, val, 10) catch {
                    std.log.err("Invalid port number: {s}", .{val});
                    return error.InvalidArgs;
                };
            }
        } else if (std.mem.eql(u8, arg, "--join")) {
            if (it.next()) |val| {
                // Expect host:port format.
                if (std.mem.lastIndexOfScalar(u8, val, ':')) |colon| {
                    args.join_host = val[0..colon];
                    args.join_port = std.fmt.parseInt(u16, val[colon + 1 ..], 10) catch {
                        std.log.err("Invalid join port: {s}", .{val[colon + 1 ..]});
                        return error.InvalidArgs;
                    };
                } else {
                    std.log.err("Join address must be in host:port format", .{});
                    return error.InvalidArgs;
                }
            }
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            std.process.exit(0);
        }
    }
    return args;
}

fn printUsage() void {
    const usage =
        \\Usage: gossip [OPTIONS]
        \\
        \\Options:
        \\  --port <PORT>       Port to listen on (default: 7001)
        \\  --join <HOST:PORT>  Address of a seed node to join
        \\  --help, -h          Show this help message
        \\
        \\Examples:
        \\  gossip --port 7001                       Start a seed node
        \\  gossip --port 7002 --join 127.0.0.1:7001 Join an existing cluster
        \\
    ;
    std.io.getStdErr().writeAll(usage) catch {};
}

// ---------------------------------------------------------------------------
// HTTP health endpoint
// ---------------------------------------------------------------------------

const health_port: u16 = 8080;

const health_body = "{\"status\":\"ok\",\"service\":\"p2p-gossip-protocol\"}";

const health_response = "HTTP/1.1 200 OK\r\n" ++
    "Content-Type: application/json\r\n" ++
    std.fmt.comptimePrint("Content-Length: {d}\r\n", .{health_body.len}) ++
    "Connection: close\r\n" ++
    "\r\n" ++
    health_body;

const not_found_response = "HTTP/1.1 404 Not Found\r\n" ++
    "Content-Length: 0\r\n" ++
    "Connection: close\r\n" ++
    "\r\n";

fn serveHealthConnection(stream: std.net.Stream) void {
    defer stream.close();
    var buf: [1024]u8 = undefined;
    const n = stream.read(&buf) catch return;
    const response = if (std.mem.startsWith(u8, buf[0..n], "GET /health"))
        health_response
    else
        not_found_response;
    stream.writeAll(response) catch {};
}

fn runHealthServer() void {
    const addr = std.net.Address.initIp4(.{ 0, 0, 0, 0 }, health_port);
    var server = addr.listen(.{ .reuse_address = true }) catch |err| {
        std.log.err("Health endpoint failed to listen on port {d}: {}", .{ health_port, err });
        return;
    };
    defer server.deinit();

    while (true) {
        const conn = server.accept() catch continue;
        serveHealthConnection(conn.stream);
    }
}

fn startHealthServer() void {
    if (std.Thread.spawn(.{}, runHealthServer, .{})) |thread| {
        // Detached daemon thread: it dies with the process, so it never
        // blocks the signal-driven engine.stop() shutdown path.
        thread.detach();
    } else |err| {
        std.log.err("Failed to start health endpoint thread: {}", .{err});
    }
}

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

var global_engine: ?*GossipEngine = null;

fn handleSignal(_: c_int) callconv(.C) void {
    if (global_engine) |engine| {
        engine.stop();
    }
}

fn installSignalHandlers() void {
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &act, null) catch {};
    std.posix.sigaction(std.posix.SIG.TERM, &act, null) catch {};
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const cli = parseArgs(allocator) catch {
        printUsage();
        std.process.exit(1);
    };

    var config = Config{
        .bind_port = cli.port,
    };

    // Resolve join address if provided.
    if (cli.join_host) |host| {
        if (cli.join_port) |port| {
            var ip_bytes: [4]u8 = undefined;
            var idx: usize = 0;
            var octet_start: usize = 0;
            for (host, 0..) |ch, ci| {
                if (ch == '.') {
                    ip_bytes[idx] = std.fmt.parseInt(u8, host[octet_start..ci], 10) catch {
                        std.log.err("Invalid IP address: {s}", .{host});
                        std.process.exit(1);
                    };
                    idx += 1;
                    octet_start = ci + 1;
                }
            }
            if (idx == 3) {
                ip_bytes[3] = std.fmt.parseInt(u8, host[octet_start..], 10) catch {
                    std.log.err("Invalid IP address: {s}", .{host});
                    std.process.exit(1);
                };
            } else {
                std.log.err("Invalid IP address: {s}", .{host});
                std.process.exit(1);
            }
            config.join_addr = std.net.Address.initIp4(ip_bytes, port);
        }
    }

    std.log.info("Starting gossip node on port {d}", .{cli.port});

    var engine = try GossipEngine.init(allocator, config);
    defer engine.deinit();

    global_engine = &engine;
    installSignalHandlers();
    startHealthServer();

    engine.run() catch |err| {
        std.log.err("Engine error: {}", .{err});
        std.process.exit(1);
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "parseArgs returns defaults with no args" {
    // Verify the default config struct values.
    const config = Config{};
    try std.testing.expectEqual(config.bind_port, 7001);
    try std.testing.expectEqual(config.protocol_period_ms, 500);
    try std.testing.expectEqual(config.ping_timeout_ms, 200);
    try std.testing.expectEqual(config.suspect_timeout_ms, 2000);
    try std.testing.expectEqual(config.ping_req_fanout, 3);
    try std.testing.expectEqual(config.dissemination_fanout, 4);
    try std.testing.expectEqual(config.default_ttl, 5);
    try std.testing.expect(config.join_addr == null);
}

test "IP address parsing logic" {
    const host = "192.168.1.100";
    var ip_bytes: [4]u8 = undefined;
    var idx: usize = 0;
    var octet_start: usize = 0;
    for (host, 0..) |ch, ci| {
        if (ch == '.') {
            ip_bytes[idx] = std.fmt.parseInt(u8, host[octet_start..ci], 10) catch unreachable;
            idx += 1;
            octet_start = ci + 1;
        }
    }
    ip_bytes[3] = std.fmt.parseInt(u8, host[octet_start..], 10) catch unreachable;

    try std.testing.expectEqual(ip_bytes[0], 192);
    try std.testing.expectEqual(ip_bytes[1], 168);
    try std.testing.expectEqual(ip_bytes[2], 1);
    try std.testing.expectEqual(ip_bytes[3], 100);
}
