const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Identifies a node in the cluster.
pub const NodeId = struct {
    addr: std.net.Address,

    pub fn eql(a: NodeId, b: NodeId) bool {
        const a4 = a.addr.in;
        const b4 = b.addr.in;
        return a4.sa.port == b4.sa.port and a4.sa.addr == b4.sa.addr;
    }

    pub fn hash(self: NodeId) u64 {
        var h = std.hash.Wyhash.init(0);
        const a4 = self.addr.in;
        h.update(mem.asBytes(&a4.sa.addr));
        h.update(mem.asBytes(&a4.sa.port));
        return h.final();
    }

    pub fn format(self: NodeId, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        const a4 = self.addr.in;
        const addr = a4.sa.addr;
        const bytes: [4]u8 = @bitCast(addr);
        const port = mem.bigToNative(u16, @bitCast(a4.sa.port));
        try writer.print("{d}.{d}.{d}.{d}:{d}", .{ bytes[0], bytes[1], bytes[2], bytes[3], port });
    }

    pub fn fromAddrPort(ip: [4]u8, port: u16) NodeId {
        return .{ .addr = std.net.Address.initIp4(ip, port) };
    }
};

/// SWIM message type.
pub const MessageType = enum(u8) {
    ping = 0,
    ack = 1,
    ping_req = 2,
    join = 3,
    leave = 4,
    suspect = 5,
    confirm = 6,
    compound = 7, // envelope carrying piggy-backed updates
};

/// A single gossip update that can be piggybacked.
pub const GossipUpdate = struct {
    kind: MessageType,
    subject: NodeId,
    incarnation: u32,
    ttl: u8,
};

/// Wire message.
pub const Message = struct {
    msg_type: MessageType,
    sender: NodeId,
    seq: u32,
    target: NodeId, // context-dependent (ping target, ack origin, etc.)
    incarnation: u32,
    updates: []GossipUpdate,

    pub fn deinit(self: *Message, allocator: Allocator) void {
        if (self.updates.len > 0) {
            allocator.free(self.updates);
        }
    }
};

// ---------------------------------------------------------------------------
// Encoding helpers (simple TLV-ish binary format)
// ---------------------------------------------------------------------------

const HEADER_SIZE: usize = 1 + 6 + 4 + 6 + 4; // type + sender(6) + seq + target(6) + incarnation
const UPDATE_SIZE: usize = 1 + 6 + 4 + 1; // kind + subject(6) + incarnation + ttl

fn encodeNodeId(buf: []u8, nid: NodeId) void {
    const a4 = nid.addr.in;
    const addr_bytes: [4]u8 = @bitCast(a4.sa.addr);
    @memcpy(buf[0..4], &addr_bytes);
    const port_bytes: [2]u8 = @bitCast(a4.sa.port);
    @memcpy(buf[4..6], &port_bytes);
}

fn decodeNodeId(buf: []const u8) NodeId {
    var addr_val: [4]u8 = undefined;
    @memcpy(&addr_val, buf[0..4]);
    var port_bytes: [2]u8 = undefined;
    @memcpy(&port_bytes, buf[4..6]);
    const port_net: u16 = @bitCast(port_bytes);
    const port = mem.bigToNative(u16, port_net);
    return NodeId.fromAddrPort(addr_val, port);
}

/// Encode a message into a caller-provided buffer. Returns the used slice.
pub fn encode(buf: []u8, msg: *const Message) ![]u8 {
    const total = HEADER_SIZE + 1 + msg.updates.len * UPDATE_SIZE;
    if (buf.len < total) return error.BufferTooSmall;

    var off: usize = 0;
    buf[off] = @intFromEnum(msg.msg_type);
    off += 1;
    encodeNodeId(buf[off..], msg.sender);
    off += 6;
    mem.writeInt(u32, buf[off..][0..4], msg.seq, .big);
    off += 4;
    encodeNodeId(buf[off..], msg.target);
    off += 6;
    mem.writeInt(u32, buf[off..][0..4], msg.incarnation, .big);
    off += 4;

    // update count
    const update_count: u8 = @intCast(msg.updates.len);
    buf[off] = update_count;
    off += 1;

    for (msg.updates) |u| {
        buf[off] = @intFromEnum(u.kind);
        off += 1;
        encodeNodeId(buf[off..], u.subject);
        off += 6;
        mem.writeInt(u32, buf[off..][0..4], u.incarnation, .big);
        off += 4;
        buf[off] = u.ttl;
        off += 1;
    }

    return buf[0..off];
}

/// Decode a message from a byte slice. Caller owns returned updates slice.
pub fn decode(allocator: Allocator, data: []const u8) !Message {
    if (data.len < HEADER_SIZE + 1) return error.MessageTooShort;

    var off: usize = 0;
    const msg_type: MessageType = @enumFromInt(data[off]);
    off += 1;
    const sender = decodeNodeId(data[off..]);
    off += 6;
    const seq = mem.readInt(u32, data[off..][0..4], .big);
    off += 4;
    const target = decodeNodeId(data[off..]);
    off += 6;
    const incarnation = mem.readInt(u32, data[off..][0..4], .big);
    off += 4;

    const update_count: usize = data[off];
    off += 1;

    if (data.len < off + update_count * UPDATE_SIZE) return error.MessageTooShort;

    const updates = try allocator.alloc(GossipUpdate, update_count);

    for (0..update_count) |i| {
        updates[i] = .{
            .kind = @enumFromInt(data[off]),
            .subject = decodeNodeId(data[off + 1 ..]),
            .incarnation = mem.readInt(u32, data[off + 7 ..][0..4], .big),
            .ttl = data[off + 11],
        };
        off += UPDATE_SIZE;
    }

    return .{
        .msg_type = msg_type,
        .sender = sender,
        .seq = seq,
        .target = target,
        .incarnation = incarnation,
        .updates = updates,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encode then decode round-trip" {
    const allocator = std.testing.allocator;
    const sender = NodeId.fromAddrPort(.{ 10, 0, 0, 1 }, 5000);
    const target = NodeId.fromAddrPort(.{ 10, 0, 0, 2 }, 5001);
    const subject = NodeId.fromAddrPort(.{ 10, 0, 0, 3 }, 5002);

    var updates_buf = [_]GossipUpdate{.{
        .kind = .join,
        .subject = subject,
        .incarnation = 1,
        .ttl = 3,
    }};

    const m = Message{
        .msg_type = .ping,
        .sender = sender,
        .seq = 42,
        .target = target,
        .incarnation = 7,
        .updates = &updates_buf,
    };

    var buf: [512]u8 = undefined;
    const encoded = try encode(&buf, &m);

    var decoded = try decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(decoded.msg_type, .ping);
    try std.testing.expectEqual(decoded.seq, 42);
    try std.testing.expectEqual(decoded.incarnation, 7);
    try std.testing.expect(decoded.sender.eql(sender));
    try std.testing.expect(decoded.target.eql(target));
    try std.testing.expectEqual(decoded.updates.len, 1);
    try std.testing.expectEqual(decoded.updates[0].kind, .join);
    try std.testing.expect(decoded.updates[0].subject.eql(subject));
}

test "encode with no updates" {
    const allocator = std.testing.allocator;
    const sender = NodeId.fromAddrPort(.{ 127, 0, 0, 1 }, 8000);

    const m = Message{
        .msg_type = .ack,
        .sender = sender,
        .seq = 1,
        .target = sender,
        .incarnation = 0,
        .updates = &[_]GossipUpdate{},
    };

    var buf: [512]u8 = undefined;
    const encoded = try encode(&buf, &m);

    var decoded = try decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(decoded.msg_type, .ack);
    try std.testing.expectEqual(decoded.updates.len, 0);
}

test "NodeId equality and hashing" {
    const a = NodeId.fromAddrPort(.{ 1, 2, 3, 4 }, 100);
    const b = NodeId.fromAddrPort(.{ 1, 2, 3, 4 }, 100);
    const c = NodeId.fromAddrPort(.{ 1, 2, 3, 4 }, 101);

    try std.testing.expect(a.eql(b));
    try std.testing.expect(!a.eql(c));
    try std.testing.expectEqual(a.hash(), b.hash());
}
