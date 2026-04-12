const std = @import("std");
const mem = std.mem;

const Allocator = mem.Allocator;

pub fn fnv1a(key: []const u8) u64 {
    // offset basis
    var hash: u64 = 0xcbf29ce484222325;

    for (key) |b| {
        hash = hash ^ b;
        // fnv prime
        hash *= 0x100000001b3;
    }

    return hash;
}

pub fn TableEntry(comptime V: type) type {
    return struct {
        short_key: u64,
        key: []const u8,
        val: V,
        hash: u64,
        /// probe sequence length, not pumpkin spice latte
        psl: usize,
    };
}

/// open-addressing hashtable with robin hood probing
pub fn Table(comptime V: type, comptime F: fn ([]const u8) u64, comptime size: usize) type {
    return struct {
        n: usize = size - 1,
        entries: [size]TableEntry(V) = std.mem.zeroes([size]TableEntry(V)),

        const GetOrPutResult = struct {
            value_ptr: *V,
            found_existing: bool,
        };

        pub fn getOrPut(self: *@This(), key: []const u8) GetOrPutResult {
            var h = F(key);
            var p = h & self.n;
            var vpsl: usize = 1;
            // key lives inside the mmap, so key.ptr[0..8] is always valid memory
            // mask off bytes past key.len so short keys are deterministic
            var sk: u64 = std.mem.readInt(u64, key.ptr[0..8], .little);
            if (key.len < 8) sk &= (@as(u64, 1) << @intCast(key.len * 8)) - 1;

            while (self.entries[p].psl != 0) {
                if (self.entries[p].hash == h and self.entries[p].short_key == sk and std.mem.eql(u8, self.entries[p].key, key)) {
                    return .{ .value_ptr = &self.entries[p].val, .found_existing = true };
                }
                // Robin Hood guarantee: if our PSL exceeds the slot's PSL,
                // the key can't exist further along — insert here instead
                if (vpsl > self.entries[p].psl) break;
                p = (p + 1) & self.n;
                vpsl += 1;
            }

            var cur_key = key;
            // not found — do Robin Hood insert, then return pointer to final slot
            while (self.entries[p].psl != 0) {
                if (vpsl > self.entries[p].psl) {
                    std.mem.swap([]const u8, &cur_key, &self.entries[p].key);
                    std.mem.swap(usize, &vpsl, &self.entries[p].psl);
                    std.mem.swap(u64, &h, &self.entries[p].hash);
                    std.mem.swap(u64, &sk, &self.entries[p].short_key);
                }

                p = (p + 1) & self.n;
                vpsl += 1;
            }

            self.entries[p] = .{ .short_key = sk, .key = cur_key, .val = std.mem.zeroes(V), .hash = h, .psl = vpsl };
            return .{ .value_ptr = &self.entries[p].val, .found_existing = false };
        }

        pub fn init(alloc: Allocator) !@This() {
            // large size makes it easier to find an entry
            return try initWithCapacity(alloc, 16384);
        }

        pub fn initWithCapacity(alloc: Allocator, comptime n: usize) !@This() {
            if (n & (n - 1) != 0) {
                @compileError("Prefer a `n` that is a power of 2");
            }

            const entries = try alloc.alloc(TableEntry(V), n);
            @memset(entries, std.mem.zeroes(TableEntry(V)));

            return .{
                .entries = entries,
                .n = n - 1,
            };
        }

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.entries);
        }

        pub fn insert(self: *@This(), key: []const u8, val: V) void {
            var h = F(key);
            var p = h & self.n;
            var vpsl: usize = 1;
            var sk: u64 = std.mem.readInt(u64, key.ptr[0..8], .little);
            if (key.len < 8) sk &= (@as(u64, 1) << @intCast(key.len * 8)) - 1;

            var cur_key = key;
            var cur_val = val;

            while (self.entries[p].psl != 0) {
                if (vpsl > self.entries[p].psl) {
                    std.mem.swap([]const u8, &cur_key, &self.entries[p].key);
                    std.mem.swap(V, &cur_val, &self.entries[p].val);
                    std.mem.swap(usize, &vpsl, &self.entries[p].psl);
                    std.mem.swap(u64, &h, &self.entries[p].hash);
                    std.mem.swap(u64, &sk, &self.entries[p].short_key);
                }

                p = (p + 1) & self.n;
                vpsl += 1;
            }

            self.entries[p] = .{ .short_key = sk, .key = cur_key, .val = cur_val, .hash = h, .psl = vpsl };
        }
    };
}
