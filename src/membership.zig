const std = @import("std");
const msg = @import("message");
const Allocator = std.mem.Allocator;

const NodeId = msg.NodeId;

/// Possible states for a peer in the membership list.
pub const PeerState = enum {
    alive,
    suspect,
    faulty,
    left,
};

/// Metadata for a single peer.
pub const PeerInfo = struct {
    id: NodeId,
    state: PeerState,
    incarnation: u32,
    state_change_ts: i64, // millisecond timestamp of last transition
};

/// Manages the set of known peers and their liveness states.
pub const MembershipList = struct {
    allocator: Allocator,
    self_id: NodeId,
    self_incarnation: u32,
    peers: std.ArrayListUnmanaged(PeerInfo),
    suspect_timeout_ms: i64,

    pub fn init(allocator: Allocator, self_id: NodeId, suspect_timeout_ms: i64) MembershipList {
        return .{
            .allocator = allocator,
            .self_id = self_id,
            .self_incarnation = 0,
            .peers = .{},
            .suspect_timeout_ms = suspect_timeout_ms,
        };
    }

    pub fn deinit(self: *MembershipList) void {
        self.peers.deinit(self.allocator);
    }

    /// Number of alive or suspect peers (not counting self).
    pub fn activeCount(self: *const MembershipList) usize {
        var n: usize = 0;
        for (self.peers.items) |p| {
            if (p.state == .alive or p.state == .suspect) n += 1;
        }
        return n;
    }

    /// Find index of a peer by NodeId, or null.
    pub fn findIndex(self: *const MembershipList, id: NodeId) ?usize {
        for (self.peers.items, 0..) |p, i| {
            if (p.id.eql(id)) return i;
        }
        return null;
    }

    /// Get peer info by NodeId.
    pub fn get(self: *const MembershipList, id: NodeId) ?PeerInfo {
        if (self.findIndex(id)) |i| return self.peers.items[i];
        return null;
    }

    /// Add or update a peer as alive. Returns true if this was a new addition.
    pub fn upsertAlive(self: *MembershipList, id: NodeId, incarnation: u32) !bool {
        if (id.eql(self.self_id)) return false;
        if (self.findIndex(id)) |i| {
            const peer = &self.peers.items[i];
            if (incarnation >= peer.incarnation) {
                peer.incarnation = incarnation;
                if (peer.state != .alive) {
                    peer.state = .alive;
                    peer.state_change_ts = nowMs();
                }
            }
            return false;
        }
        try self.peers.append(self.allocator, .{
            .id = id,
            .state = .alive,
            .incarnation = incarnation,
            .state_change_ts = nowMs(),
        });
        return true;
    }

    /// Mark a peer as suspect.
    pub fn markSuspect(self: *MembershipList, id: NodeId, incarnation: u32) void {
        if (self.findIndex(id)) |i| {
            const peer = &self.peers.items[i];
            if (incarnation >= peer.incarnation and peer.state == .alive) {
                peer.state = .suspect;
                peer.incarnation = incarnation;
                peer.state_change_ts = nowMs();
            }
        }
    }

    /// Mark a peer as faulty (confirmed dead).
    pub fn markFaulty(self: *MembershipList, id: NodeId, incarnation: u32) void {
        if (self.findIndex(id)) |i| {
            const peer = &self.peers.items[i];
            if (incarnation >= peer.incarnation) {
                peer.state = .faulty;
                peer.incarnation = incarnation;
                peer.state_change_ts = nowMs();
            }
        }
    }

    /// Record a graceful leave.
    pub fn markLeft(self: *MembershipList, id: NodeId) void {
        if (self.findIndex(id)) |i| {
            self.peers.items[i].state = .left;
            self.peers.items[i].state_change_ts = nowMs();
        }
    }

    /// Remove peers that have been faulty or left for longer than the timeout.
    pub fn reapDead(self: *MembershipList) void {
        const now = nowMs();
        var i: usize = 0;
        while (i < self.peers.items.len) {
            const p = self.peers.items[i];
            if ((p.state == .faulty or p.state == .left) and (now - p.state_change_ts > self.suspect_timeout_ms * 4)) {
                _ = self.peers.swapRemove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Promote suspects that have timed out to faulty. Returns a list of
    /// newly confirmed-faulty NodeIds (caller owns the slice).
    pub fn promoteExpiredSuspects(self: *MembershipList) ![]NodeId {
        const now = nowMs();
        var result = std.ArrayList(NodeId).init(self.allocator);
        for (self.peers.items) |*p| {
            if (p.state == .suspect and (now - p.state_change_ts > self.suspect_timeout_ms)) {
                p.state = .faulty;
                p.state_change_ts = now;
                try result.append(p.id);
            }
        }
        return result.toOwnedSlice();
    }

    /// Select up to `k` random alive/suspect peers, excluding `exclude`.
    pub fn selectRandomPeers(self: *MembershipList, k: usize, exclude: ?NodeId) ![]NodeId {
        var candidates = std.ArrayList(NodeId).init(self.allocator);
        defer candidates.deinit();

        for (self.peers.items) |p| {
            if (p.state != .alive and p.state != .suspect) continue;
            if (exclude) |ex| {
                if (p.id.eql(ex)) continue;
            }
            try candidates.append(p.id);
        }

        // Fisher-Yates shuffle the candidate list.
        var prng = std.Random.DefaultPrng.init(@bitCast(nowMs()));
        const rand = prng.random();
        const items = candidates.items;
        if (items.len > 1) {
            var i: usize = items.len - 1;
            while (i > 0) : (i -= 1) {
                const j = rand.uintLessThan(usize, i + 1);
                const tmp = items[i];
                items[i] = items[j];
                items[j] = tmp;
            }
        }

        const count = @min(k, items.len);
        const result = try self.allocator.alloc(NodeId, count);
        @memcpy(result, items[0..count]);
        return result;
    }
};

fn nowMs() i64 {
    return @divFloor(std.time.milliTimestamp(), 1);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "MembershipList add and find peers" {
    const allocator = std.testing.allocator;
    const self_id = NodeId.fromAddrPort(.{ 127, 0, 0, 1 }, 5000);

    var ml = MembershipList.init(allocator, self_id, 2000);
    defer ml.deinit();

    const peer_a = NodeId.fromAddrPort(.{ 10, 0, 0, 1 }, 6000);
    const peer_b = NodeId.fromAddrPort(.{ 10, 0, 0, 2 }, 6001);

    const added_a = try ml.upsertAlive(peer_a, 0);
    try std.testing.expect(added_a);

    const added_b = try ml.upsertAlive(peer_b, 0);
    try std.testing.expect(added_b);

    // Re-adding is not a new addition.
    const added_a2 = try ml.upsertAlive(peer_a, 1);
    try std.testing.expect(!added_a2);

    try std.testing.expectEqual(ml.activeCount(), 2);
    try std.testing.expect(ml.get(peer_a) != null);
}

test "MembershipList state transitions" {
    const allocator = std.testing.allocator;
    const self_id = NodeId.fromAddrPort(.{ 127, 0, 0, 1 }, 5000);

    var ml = MembershipList.init(allocator, self_id, 2000);
    defer ml.deinit();

    const peer = NodeId.fromAddrPort(.{ 10, 0, 0, 1 }, 6000);
    _ = try ml.upsertAlive(peer, 0);

    ml.markSuspect(peer, 0);
    try std.testing.expectEqual(ml.get(peer).?.state, .suspect);

    ml.markFaulty(peer, 1);
    try std.testing.expectEqual(ml.get(peer).?.state, .faulty);
    try std.testing.expectEqual(ml.activeCount(), 0);
}

test "MembershipList ignores self" {
    const allocator = std.testing.allocator;
    const self_id = NodeId.fromAddrPort(.{ 127, 0, 0, 1 }, 5000);

    var ml = MembershipList.init(allocator, self_id, 2000);
    defer ml.deinit();

    const added = try ml.upsertAlive(self_id, 0);
    try std.testing.expect(!added);
    try std.testing.expectEqual(ml.activeCount(), 0);
}

test "MembershipList selectRandomPeers" {
    const allocator = std.testing.allocator;
    const self_id = NodeId.fromAddrPort(.{ 127, 0, 0, 1 }, 5000);

    var ml = MembershipList.init(allocator, self_id, 2000);
    defer ml.deinit();

    for (0..5) |i| {
        const port: u16 = @intCast(6000 + i);
        _ = try ml.upsertAlive(NodeId.fromAddrPort(.{ 10, 0, 0, 1 }, port), 0);
    }

    const selected = try ml.selectRandomPeers(3, null);
    defer allocator.free(selected);
    try std.testing.expectEqual(selected.len, 3);
}

test "MembershipList leave" {
    const allocator = std.testing.allocator;
    const self_id = NodeId.fromAddrPort(.{ 127, 0, 0, 1 }, 5000);
    var ml = MembershipList.init(allocator, self_id, 2000);
    defer ml.deinit();

    const peer = NodeId.fromAddrPort(.{ 10, 0, 0, 1 }, 6000);
    _ = try ml.upsertAlive(peer, 0);
    ml.markLeft(peer);
    try std.testing.expectEqual(ml.get(peer).?.state, .left);
    try std.testing.expectEqual(ml.activeCount(), 0);
}
