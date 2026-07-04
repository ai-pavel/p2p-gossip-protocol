const std = @import("std");
const msg = @import("message");
const transport_mod = @import("transport");
const membership_mod = @import("membership");
const Allocator = std.mem.Allocator;

const NodeId = msg.NodeId;
const Message = msg.Message;
const GossipUpdate = msg.GossipUpdate;
const MessageType = msg.MessageType;
const Transport = transport_mod.Transport;
const MembershipList = membership_mod.MembershipList;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub const Config = struct {
    bind_port: u16 = 7001,
    protocol_period_ms: u64 = 500,
    ping_timeout_ms: u64 = 200,
    suspect_timeout_ms: i64 = 2000,
    ping_req_fanout: usize = 3,
    dissemination_fanout: usize = 4,
    default_ttl: u8 = 5,
    join_addr: ?std.net.Address = null,
};

// ---------------------------------------------------------------------------
// Pending-ack tracking
// ---------------------------------------------------------------------------

const PendingPing = struct {
    target: NodeId,
    seq: u32,
    deadline_ms: i64,
    indirect: bool, // was this an indirect probe?
};

// ---------------------------------------------------------------------------
// GossipEngine
// ---------------------------------------------------------------------------

pub const GossipEngine = struct {
    allocator: Allocator,
    config: Config,
    self_id: NodeId,
    transport: Transport,
    members: MembershipList,
    seq_counter: u32,
    pending_pings: std.ArrayListUnmanaged(PendingPing),
    update_queue: std.ArrayListUnmanaged(GossipUpdate),
    running: bool,

    pub fn init(allocator: Allocator, config: Config) !GossipEngine {
        const tp = try Transport.init(allocator, config.bind_port);
        const self_id = NodeId.fromAddrPort(.{ 127, 0, 0, 1 }, config.bind_port);

        var engine = GossipEngine{
            .allocator = allocator,
            .config = config,
            .self_id = self_id,
            .transport = tp,
            .members = MembershipList.init(allocator, self_id, config.suspect_timeout_ms),
            .seq_counter = 0,
            .pending_pings = .{},
            .update_queue = .{},
            .running = false,
        };

        // If a join address was provided, add the seed and send a join message.
        if (config.join_addr) |join_addr| {
            const seed_id = NodeId{ .addr = join_addr };
            _ = try engine.members.upsertAlive(seed_id, 0);

            const join_msg = Message{
                .msg_type = .join,
                .sender = self_id,
                .seq = engine.nextSeq(),
                .target = seed_id,
                .incarnation = 0,
                .updates = &[_]GossipUpdate{},
            };
            engine.transport.sendMsg(join_addr, &join_msg) catch |err| {
                std.log.warn("Failed to send join to seed: {}", .{err});
            };
        }

        return engine;
    }

    pub fn deinit(self: *GossipEngine) void {
        self.pending_pings.deinit(self.allocator);
        self.update_queue.deinit(self.allocator);
        self.members.deinit();
        self.transport.deinit();
    }

    fn nextSeq(self: *GossipEngine) u32 {
        const s = self.seq_counter;
        self.seq_counter +%= 1;
        return s;
    }

    // ------------------------------------------------------------------
    // Update queue (infection-style dissemination)
    // ------------------------------------------------------------------

    /// Enqueue a gossip update to be piggybacked onto outgoing messages.
    pub fn enqueueUpdate(self: *GossipEngine, update: GossipUpdate) !void {
        try self.update_queue.append(self.allocator, update);
    }

    /// Drain up to `fanout` updates, decrementing TTL. Expired updates are
    /// removed. Caller owns returned slice.
    fn drainUpdates(self: *GossipEngine) ![]GossipUpdate {
        const count = @min(self.config.dissemination_fanout, self.update_queue.items.len);
        if (count == 0) return &[_]GossipUpdate{};

        const result = try self.allocator.alloc(GossipUpdate, count);
        var kept: usize = 0;

        for (0..count) |i| {
            var u = self.update_queue.items[i];
            if (u.ttl > 0) {
                u.ttl -= 1;
                result[kept] = u;
                self.update_queue.items[i] = u; // update in-place for re-send
                kept += 1;
            }
        }

        // Remove expired (TTL == 0 after decrement) entries from the front.
        var remove_count: usize = 0;
        for (self.update_queue.items) |u| {
            if (u.ttl == 0) {
                remove_count += 1;
            } else {
                break;
            }
        }
        if (remove_count > 0) {
            const items = self.update_queue.items;
            const remaining = items.len - remove_count;
            if (remaining > 0) {
                std.mem.copyForwards(GossipUpdate, items[0..remaining], items[remove_count..]);
            }
            self.update_queue.items.len = remaining;
        }

        if (kept < result.len) {
            // Shrink; free is safe because we own the slice.
            const shrunk = try self.allocator.realloc(result, kept);
            return shrunk;
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Sending helpers
    // ------------------------------------------------------------------

    fn sendPing(self: *GossipEngine, target: NodeId) !void {
        const seq = self.nextSeq();
        const updates = try self.drainUpdates();
        defer if (updates.len > 0) self.allocator.free(updates);

        const m = Message{
            .msg_type = .ping,
            .sender = self.self_id,
            .seq = seq,
            .target = target,
            .incarnation = self.members.self_incarnation,
            .updates = updates,
        };
        try self.transport.sendMsg(target.addr, &m);

        const now = std.time.milliTimestamp();
        try self.pending_pings.append(self.allocator, .{
            .target = target,
            .seq = seq,
            .deadline_ms = now + @as(i64, @intCast(self.config.ping_timeout_ms)),
            .indirect = false,
        });
    }

    fn sendAck(self: *GossipEngine, dest: NodeId, seq: u32) !void {
        const updates = try self.drainUpdates();
        defer if (updates.len > 0) self.allocator.free(updates);

        const m = Message{
            .msg_type = .ack,
            .sender = self.self_id,
            .seq = seq,
            .target = dest,
            .incarnation = self.members.self_incarnation,
            .updates = updates,
        };
        try self.transport.sendMsg(dest.addr, &m);
    }

    fn sendPingReq(self: *GossipEngine, via: NodeId, target: NodeId) !void {
        const seq = self.nextSeq();
        const m = Message{
            .msg_type = .ping_req,
            .sender = self.self_id,
            .seq = seq,
            .target = target,
            .incarnation = self.members.self_incarnation,
            .updates = &[_]GossipUpdate{},
        };
        try self.transport.sendMsg(via.addr, &m);
    }

    fn broadcastLeave(self: *GossipEngine) !void {
        for (self.members.peers.items) |p| {
            if (p.state != .alive and p.state != .suspect) continue;
            const m = Message{
                .msg_type = .leave,
                .sender = self.self_id,
                .seq = self.nextSeq(),
                .target = p.id,
                .incarnation = self.members.self_incarnation,
                .updates = &[_]GossipUpdate{},
            };
            self.transport.sendMsg(p.id.addr, &m) catch {};
        }
    }

    // ------------------------------------------------------------------
    // Incoming message handling
    // ------------------------------------------------------------------

    fn handleMessage(self: *GossipEngine, message: *Message) !void {
        // Process piggybacked updates.
        for (message.updates) |u| {
            try self.applyUpdate(u);
        }

        // Ensure sender is known.
        _ = try self.members.upsertAlive(message.sender, message.incarnation);

        switch (message.msg_type) {
            .ping => {
                try self.sendAck(message.sender, message.seq);
            },
            .ack => {
                self.resolveAck(message.seq, message.sender);
            },
            .ping_req => {
                // Indirect ping: ping the target on behalf of the requester.
                try self.sendPing(message.target);
            },
            .join => {
                std.log.info("Node {} joined", .{message.sender});
                try self.enqueueUpdate(.{
                    .kind = .join,
                    .subject = message.sender,
                    .incarnation = message.incarnation,
                    .ttl = self.config.default_ttl,
                });
                // Send an ack so the joiner knows we received it.
                try self.sendAck(message.sender, message.seq);
            },
            .leave => {
                std.log.info("Node {} left gracefully", .{message.sender});
                self.members.markLeft(message.sender);
                try self.enqueueUpdate(.{
                    .kind = .leave,
                    .subject = message.sender,
                    .incarnation = message.incarnation,
                    .ttl = self.config.default_ttl,
                });
            },
            .suspect => {
                self.members.markSuspect(message.target, message.incarnation);
            },
            .confirm => {
                self.members.markFaulty(message.target, message.incarnation);
            },
            .compound => {
                // Updates already processed above; nothing else to do.
            },
        }
    }

    fn applyUpdate(self: *GossipEngine, update: GossipUpdate) !void {
        switch (update.kind) {
            .join => {
                _ = try self.members.upsertAlive(update.subject, update.incarnation);
            },
            .leave => {
                self.members.markLeft(update.subject);
            },
            .suspect => {
                // If the suspect is us, refute by bumping incarnation.
                if (update.subject.eql(self.self_id)) {
                    if (update.incarnation >= self.members.self_incarnation) {
                        self.members.self_incarnation = update.incarnation + 1;
                        try self.enqueueUpdate(.{
                            .kind = .join, // alive refutation
                            .subject = self.self_id,
                            .incarnation = self.members.self_incarnation,
                            .ttl = self.config.default_ttl,
                        });
                    }
                } else {
                    self.members.markSuspect(update.subject, update.incarnation);
                }
            },
            .confirm => {
                self.members.markFaulty(update.subject, update.incarnation);
            },
            else => {},
        }
    }

    fn resolveAck(self: *GossipEngine, seq: u32, _: NodeId) void {
        var i: usize = 0;
        while (i < self.pending_pings.items.len) {
            if (self.pending_pings.items[i].seq == seq) {
                _ = self.pending_pings.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    // ------------------------------------------------------------------
    // Protocol period tick
    // ------------------------------------------------------------------

    fn tick(self: *GossipEngine) !void {
        // 1. Check for timed-out pings.
        const now = std.time.milliTimestamp();
        var i: usize = 0;
        while (i < self.pending_pings.items.len) {
            const pp = self.pending_pings.items[i];
            if (now >= pp.deadline_ms) {
                _ = self.pending_pings.swapRemove(i);
                if (!pp.indirect) {
                    // Send indirect probes via random peers.
                    const helpers = try self.members.selectRandomPeers(self.config.ping_req_fanout, pp.target);
                    defer self.allocator.free(helpers);
                    for (helpers) |h| {
                        self.sendPingReq(h, pp.target) catch {};
                    }
                    // Mark suspect if still no ack after another timeout.
                    self.members.markSuspect(pp.target, 0);
                    try self.enqueueUpdate(.{
                        .kind = .suspect,
                        .subject = pp.target,
                        .incarnation = 0,
                        .ttl = self.config.default_ttl,
                    });
                }
            } else {
                i += 1;
            }
        }

        // 2. Promote expired suspects -> faulty.
        const newly_faulty = try self.members.promoteExpiredSuspects();
        defer self.allocator.free(newly_faulty);
        for (newly_faulty) |nid| {
            try self.enqueueUpdate(.{
                .kind = .confirm,
                .subject = nid,
                .incarnation = 0,
                .ttl = self.config.default_ttl,
            });
        }

        // 3. Reap long-dead entries.
        self.members.reapDead();

        // 4. Pick a random peer and ping it.
        if (self.members.activeCount() > 0) {
            const targets = try self.members.selectRandomPeers(1, null);
            defer self.allocator.free(targets);
            if (targets.len > 0) {
                self.sendPing(targets[0]) catch |err| {
                    std.log.warn("ping failed: {}", .{err});
                };
            }
        }
    }

    // ------------------------------------------------------------------
    // Receive loop (non-blocking drain)
    // ------------------------------------------------------------------

    fn drainIncoming(self: *GossipEngine) !void {
        var count: usize = 0;
        while (count < 64) : (count += 1) {
            const result = try self.transport.recv();
            if (result == null) break;

            const r = result.?;
            defer self.allocator.free(r.data);

            var message = msg.decode(self.allocator, r.data) catch |err| {
                std.log.warn("bad packet: {}", .{err});
                continue;
            };
            defer message.deinit(self.allocator);

            try self.handleMessage(&message);
        }
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Run the protocol loop. Blocks until `stop()` is called.
    pub fn run(self: *GossipEngine) !void {
        self.running = true;
        std.log.info("gossip node started on port {d} (id={any})", .{ self.config.bind_port, self.self_id });

        while (self.running) {
            try self.drainIncoming();
            try self.tick();
            std.time.sleep(self.config.protocol_period_ms * std.time.ns_per_ms);
        }

        // Graceful leave: notify peers.
        try self.broadcastLeave();
        // Drain any final acks.
        std.time.sleep(100 * std.time.ns_per_ms);
        try self.drainIncoming();
        std.log.info("gossip node stopped", .{});
    }

    /// Signal the engine to stop after the current period.
    pub fn stop(self: *GossipEngine) void {
        self.running = false;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "GossipEngine init and deinit" {
    const allocator = std.testing.allocator;
    var engine = try GossipEngine.init(allocator, .{
        .bind_port = 0, // ephemeral
    });
    defer engine.deinit();

    try std.testing.expectEqual(engine.members.activeCount(), 0);
}

test "GossipEngine enqueue and drain updates" {
    const allocator = std.testing.allocator;
    var engine = try GossipEngine.init(allocator, .{
        .bind_port = 0,
        .dissemination_fanout = 2,
        .default_ttl = 2,
    });
    defer engine.deinit();

    const peer = NodeId.fromAddrPort(.{ 10, 0, 0, 1 }, 6000);
    try engine.enqueueUpdate(.{
        .kind = .join,
        .subject = peer,
        .incarnation = 0,
        .ttl = 2,
    });
    try engine.enqueueUpdate(.{
        .kind = .leave,
        .subject = peer,
        .incarnation = 0,
        .ttl = 1,
    });

    const drained = try engine.drainUpdates();
    defer if (drained.len > 0) allocator.free(drained);

    try std.testing.expectEqual(drained.len, 2);
    // TTL should have been decremented.
    try std.testing.expectEqual(drained[0].ttl, 1);
    try std.testing.expectEqual(drained[1].ttl, 0);
}

test "GossipEngine resolveAck removes pending ping" {
    const allocator = std.testing.allocator;
    var engine = try GossipEngine.init(allocator, .{ .bind_port = 0 });
    defer engine.deinit();

    const target = NodeId.fromAddrPort(.{ 10, 0, 0, 1 }, 6000);
    try engine.pending_pings.append(allocator, .{
        .target = target,
        .seq = 42,
        .deadline_ms = std.time.milliTimestamp() + 5000,
        .indirect = false,
    });

    engine.resolveAck(42, target);
    try std.testing.expectEqual(engine.pending_pings.items.len, 0);
}
