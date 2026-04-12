const std = @import("std");
const mmap = @import("mmap.zig");
const hash = @import("hash.zig");
const swar = @import("swar.zig");
const tracy = @import("tracy");

const Stats = struct {
    // ordered to avoid padding
    sum: i64,
    n: u32,
    min: i32,
    max: i32,
};

fn hash_fn(k: []const u8) u64 {
    return std.hash.Wyhash.hash(0, k);
}

// change this to your CPU's logical core count
const PARALLELISM = 14;

const HashTable: type = hash.Table([]const u8, Stats, hash_fn, 16384);
const TableEntry: type = hash.TableEntry([]const u8, Stats);

const ThreadContext = struct { bytes: []const u8, map: HashTable = .{} };

fn updateRecord(map: *HashTable, key: []const u8, temp: i32) !void {
    const res = map.getOrPut(key);
    if (res.found_existing) {
        res.value_ptr.sum += temp;
        if (temp < res.value_ptr.min) res.value_ptr.min = temp;
        if (temp > res.value_ptr.max) res.value_ptr.max = temp;
        res.value_ptr.n += 1;
    } else {
        res.value_ptr.* = Stats{
            .min = temp,
            .max = temp,
            .sum = temp,
            .n = 1,
        };
    }
}

fn parseRange(ctx: *ThreadContext) !void {
    tracy.setThreadName("parseRange");
    defer tracy.message("Graceful parseRange thread exist", .{});
    const zone = tracy.beginZone(@src(), .{ .name = "parseRange" });
    defer zone.end();

    ctx.map = .{};

    var i: usize = 0;
    while (i < ctx.bytes.len) {
        const find_zone = tracy.beginZone(@src(), .{ .name = "findSIMD" });
        // very slightly (~20ms) slower than `findSIMD`. no idea why.
        const result = swar.find(ctx.bytes[i..], ';');
        find_zone.end();

        // parse a line: first, find the ;
        if (result) |j| {
            // temp can be a few cases: X.X, -X.X, XX.X, -XX.X
            const first = ctx.bytes[i + j + 1];
            const second = ctx.bytes[i + j + 2];
            const third = ctx.bytes[i + j + 3];
            const fourth = ctx.bytes[i + j + 4];

            const neg = first == '-';
            const fifth = @intFromBool(neg) * ctx.bytes[i + j + 5];

            const temp_len: usize = if (neg)
                if (third == '.') 4 else 5
            else if (second == '.') 3 else 4;

            const temp: i32 = switch (temp_len) {
                3 => @as(i32, first) * 10 + @as(i32, third) - 0x210,
                4 => if (first == '-')
                    -(@as(i32, second) * 10 + @as(i32, fourth) - 0x210)
                else
                    @as(i32, first) * 100 + @as(i32, second) * 10 + @as(i32, fourth) - 0x14d0,
                5 => -(@as(i32, second) * 100 + @as(i32, third) * 10 + @as(i32, fifth) - 0x14d0),
                else => unreachable,
            };

            const update_zone = tracy.beginZone(@src(), .{ .name = "updateRecord" });
            try updateRecord(&ctx.map, ctx.bytes[i .. i + j], temp);
            update_zone.end();

            i += j + 2 + temp_len;
        } else {
            // we should always find a ';', so panic if we don't
            unreachable;
        }
    }
}

fn stringLessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

// integer rounding trick: add half the divisor for pos, subtract for neg
fn roundedMeanTenths(sum: i64, count: u32) i64 {
    const denom: i64 = @intCast(count);
    const half: i64 = @intCast(count / 2);
    if (sum >= 0) {
        return @divTrunc(sum + half, denom);
    }
    return @divTrunc(sum - half, denom);
}

fn writeTenths(writer: anytype, value: i64) !void {
    if (value < 0) try writer.writeByte('-');
    const abs_value: u64 = @intCast(if (value < 0) -value else value);
    try writer.print("{d}.{d:0>1}", .{ abs_value / 10, abs_value % 10 });
}

const CITY_IDX_HASH_SIZE = 2048;
fn emitResults(
    writer: anytype,
    names: []const []const u8,
    city_idx: *hash.Table([]const u8, usize, hash.fnv1a, CITY_IDX_HASH_SIZE),
    stats: []const Stats,
) !void {
    try writer.writeByte('{');
    for (names, 0..) |city, i| {
        if (i != 0) {
            try writer.writeByte(',');
            try writer.writeByte(' ');
        }

        const idx = city_idx.getOrPut(city).value_ptr.*;
        const stat = stats[idx];

        try writer.print("{s}=", .{city});
        try writeTenths(writer, stat.min);
        try writer.writeByte('/');
        try writeTenths(writer, roundedMeanTenths(stat.sum, stat.n));
        try writer.writeByte('/');
        try writeTenths(writer, stat.max);
    }

    try writer.writeByte('}');
    try writer.writeByte('\n');
}

fn parallelAggregate(
    tid: usize,
    n_threads: usize,
    maps: []const ThreadContext,
    result: *hash.Table([]const u8, Stats, hash_fn, 2048),
) void {
    result.* = .{};

    for (maps) |ctx| {
        for (ctx.map.entries) |e| {
            if (e.psl == 0) continue;
            if (hash_fn(e.key) % n_threads != tid) continue;

            const res = result.getOrPut(e.key);
            if (res.found_existing) {
                res.value_ptr.* = .{
                    .min = @min(res.value_ptr.min, e.val.min),
                    .max = @max(res.value_ptr.max, e.val.max),
                    .n = res.value_ptr.n + e.val.n,
                    .sum = res.value_ptr.sum + e.val.sum,
                };
            } else {
                res.value_ptr.* = e.val;
            }
        }
    }
}

pub fn main() !void {
    const filename = "1B.txt";
    tracy.setThreadName("Main");
    defer tracy.message("Graceful main thread exit", .{});

    const file = std.fs.cwd().openFile(filename, .{}) catch |e| {
        std.debug.print("{any}", .{e});
        @panic("Error: failed to open file.");
    };
    defer file.close();

    const thread_count = PARALLELISM;
    var threads = try std.heap.smp_allocator.alloc(std.Thread, thread_count);
    defer std.heap.smp_allocator.free(threads);

    const pager_zone = tracy.beginZone(@src(), .{ .name = "MmapPager.init" });
    var pager = try mmap.MmapPager.init(file.handle);
    defer pager.deinit();
    pager_zone.end();

    // create chunks naively: N/k for now
    const chunksize = pager.ptr.len / thread_count;

    const contexts = try std.heap.smp_allocator.alloc(ThreadContext, thread_count);
    defer std.heap.smp_allocator.free(contexts);

    var spawned: usize = 0;
    var cursor: usize = 0;
    for (0..thread_count) |i| {
        if (cursor >= pager.ptr.len) break;

        const raw_end = cursor + chunksize;
        var end = @min(raw_end, pager.ptr.len);

        if (i == thread_count - 1) {
            end = pager.ptr.len;
        } else {
            end += swar.find(pager.ptr[end..], '\n').?;
            if (end < pager.ptr.len) end += 1; // go past the newline
        }

        const idx = spawned;

        contexts[idx] = .{ .bytes = pager.ptr[cursor..end], .map = undefined };
        threads[idx] = try std.Thread.spawn(.{}, parseRange, .{&contexts[idx]});

        spawned += 1;
        cursor = end;
    }

    const thread_zone = tracy.beginZone(@src(), .{ .name = "joinThreads" });
    for (threads[0..spawned]) |thread| {
        thread.join();
    }
    thread_zone.end();

    var ct: usize = 0;
    var city_idx = hash.Table([]const u8, usize, hash.fnv1a, CITY_IDX_HASH_SIZE){};

    var stats: [512]Stats = std.mem.zeroes([512]Stats);

    var names = try std.ArrayList([]const u8).initCapacity(std.heap.smp_allocator, 512);
    defer names.deinit(std.heap.smp_allocator);

    const AggTable = hash.Table([]const u8, Stats, hash_fn, 2048);
    const partial_results = try std.heap.smp_allocator.alloc(AggTable, thread_count);
    defer std.heap.smp_allocator.free(partial_results);

    for (0..thread_count) |i| {
        threads[i] = try std.Thread.spawn(
            .{},
            parallelAggregate,
            .{ i, thread_count, contexts, &partial_results[i] },
        );
    }
    for (threads) |t| t.join();

    var i: usize = 0; // loop over stats
    for (partial_results) |*agg| {
        for (agg.entries) |e| {
            if (e.psl == 0) continue;

            const res = city_idx.getOrPut(e.key);
            if (res.found_existing) {
                // threads own disjoint city sets
                unreachable;
            } else {
                res.value_ptr.* = ct;
                try names.append(std.heap.smp_allocator, e.key);
                stats[i] = e.val;

                ct += 1;
                i += 1;
            }
        }
    }

    std.mem.sort([]const u8, names.items, {}, stringLessThan);

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    // profiled, this only takes ~300μs
    try emitResults(stdout, names.items, &city_idx, &stats);
    try stdout.flush();
}

// useful to validate correctness. the "checksum" is just the sum of all the sums. it's crude but good enough.
fn checksumFile(filename: []const u8) !i64 {
    const file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();

    var pager = try mmap.MmapPager.init(file.handle);
    defer pager.deinit();

    var ctx = ThreadContext{ .bytes = pager.ptr, .map = .{ .n = 16383 } };

    try parseRange(&ctx);

    var total: i64 = 0;
    for (ctx.map.entries) |entry| {
        total += entry.val.sum;
    }

    return total;
}

test "checksum for 1M.txt" {
    try std.testing.expectEqual(@as(i64, 178271700), try checksumFile("1M.txt"));
}

test "checksum for 10M.txt" {
    try std.testing.expectEqual(@as(i64, 1783055396), try checksumFile("10M.txt"));
}

test "checksum for 100M.txt" {
    try std.testing.expectEqual(@as(i64, 17829259159), try checksumFile("100M.txt"));
}
