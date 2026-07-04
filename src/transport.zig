const std = @import("std");
const msg = @import("message");
const Allocator = std.mem.Allocator;
const posix = std.posix;

pub const max_packet_size: usize = 1400; // fits in a single Ethernet frame

/// A thin wrapper around a non-blocking UDP socket.
pub const Transport = struct {
    sock: posix.socket_t,
    self_addr: std.net.Address,
    allocator: Allocator,

    pub fn init(allocator: Allocator, bind_port: u16) !Transport {
        const addr = std.net.Address.initIp4(.{ 0, 0, 0, 0 }, bind_port);
        const sock = try posix.socket(posix.AF.INET, posix.SOCK.DGRAM | posix.SOCK.NONBLOCK, 0);
        errdefer posix.close(sock);

        // Allow address reuse so we can restart quickly.
        try posix.setsockopt(sock, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        try posix.bind(sock, &addr.any, addr.getOsSockLen());

        return .{
            .sock = sock,
            .self_addr = addr,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Transport) void {
        posix.close(self.sock);
    }

    /// Send a pre-encoded byte slice to `dest`.
    pub fn sendTo(self: *Transport, dest: std.net.Address, data: []const u8) !void {
        _ = try posix.sendto(self.sock, data, 0, &dest.any, dest.getOsSockLen());
    }

    /// Send a `Message` to `dest`. Encodes into a stack buffer.
    pub fn sendMsg(self: *Transport, dest: std.net.Address, message: *const msg.Message) !void {
        var buf: [max_packet_size]u8 = undefined;
        const encoded = try msg.encode(&buf, message);
        try self.sendTo(dest, encoded);
    }

    /// Non-blocking receive. Returns null when there is nothing to read.
    pub fn recv(self: *Transport) !?RecvResult {
        var buf: [max_packet_size]u8 = undefined;
        var src_addr: posix.sockaddr.storage = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);

        const n = posix.recvfrom(self.sock, &buf, 0, @ptrCast(&src_addr), &addr_len) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };

        const data = try self.allocator.alloc(u8, n);
        @memcpy(data, buf[0..n]);

        return .{
            .data = data,
            .from = std.net.Address{ .any = @as(*const posix.sockaddr, @ptrCast(&src_addr)).* },
        };
    }

    pub const RecvResult = struct {
        data: []u8,
        from: std.net.Address,
    };
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Transport bind and self-send round-trip" {
    const allocator = std.testing.allocator;

    var t = try Transport.init(allocator, 0); // ephemeral port
    defer t.deinit();

    // Discover the actual bound port.
    var bound_addr: posix.sockaddr.storage = undefined;
    var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
    try posix.getsockname(t.sock, @ptrCast(&bound_addr), &addr_len);
    const actual: *const posix.sockaddr.in = @ptrCast(@alignCast(&bound_addr));
    const port = std.mem.bigToNative(u16, @bitCast(actual.port));

    const dest = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port);
    const payload = "hello gossip";
    try t.sendTo(dest, payload);

    // Give the loopback a moment (usually instant).
    std.time.sleep(10 * std.time.ns_per_ms);

    const result = try t.recv();
    try std.testing.expect(result != null);
    defer allocator.free(result.?.data);
    try std.testing.expectEqualSlices(u8, payload, result.?.data);
}

test "Transport recv returns null when empty" {
    const allocator = std.testing.allocator;
    var t = try Transport.init(allocator, 0);
    defer t.deinit();

    const result = try t.recv();
    try std.testing.expect(result == null);
}
