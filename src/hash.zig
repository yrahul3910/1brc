const std = @import("std");

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

fn TableEntry(comptime K: type, comptime V: type) type {
    return struct {
        key: K,
        val: V,
        /// probe sequence length, not pumpkin spice latte
        psl: usize,
    };
}

/// open-addressing hashtable with robin hood probing
fn Table(comptime K: type, comptime V: type, comptime F: fn (K) u64) type {
    return struct {
        entries: []TableEntry(K, V),

        fn insert(self: *@This(), key: K, val: V) void {
            var p = F(key) % self.entries.len;
            var vpsl: usize = 1;

            var cur_key = key;
            var cur_val = val;

            while (self.entries[p].psl != 0) {
                if (vpsl > self.entries[p].psl) {
                    std.mem.swap(K, &cur_key, &self.entries[p].key);
                    std.mem.swap(V, &cur_val, &self.entries[p].val);
                    std.mem.swap(usize, &vpsl, &self.entries[p].psl);
                }

                p = (p + 1) % self.entries.len;
                vpsl += 1;
            }

            self.entries[p] = .{ .key = cur_key, .val = cur_val, .psl = vpsl };
        }
    };
}
