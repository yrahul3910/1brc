const std = @import("std");
const mmap = @import("mmap.zig");

const Stats = struct {
    min: i32,
    max: i32,
    sum: i64,
    n: u32,
};

const ThreadContext = struct {
    bytes: []const u8,
    map: std.StringHashMap(Stats),
};

fn updateRecord(map: *std.StringHashMap(Stats), key: []const u8, temp: i32) !void {
    const res = try map.getOrPut(key);
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
    var parsing_temp = false;
    var cs: usize = 0; // where did the current city start?
    var semi: usize = 0; // where did the temp start? points to ;
    var multiplier: i32 = 1;
    var cur_temp: i32 = 0;

    for (ctx.bytes, 0..ctx.bytes.len) |c, i| {
        switch (c) {
            ';' => {
                parsing_temp = true;
                semi = i;
            },
            '\n' => {
                const newt = multiplier * cur_temp;

                try updateRecord(&ctx.map, ctx.bytes[cs..semi], newt);

                multiplier = 1;
                cur_temp = 0;
                parsing_temp = false;
                cs = i + 1;
            },
            '-' => {
                // can also appear within city names
                if (parsing_temp) {
                    multiplier = -1;
                }
            },
            // opt: all temp values have 1 decimal places, divide by 10 at the end
            '0'...'9' => {
                if (parsing_temp) {
                    cur_temp = 10 * cur_temp + (c - '0');
                }
            },
            else => {},
        }
    }

    if (parsing_temp) {
        const newt = multiplier * cur_temp;
        try updateRecord(&ctx.map, ctx.bytes[cs..semi], newt);
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

fn emitResults(
    writer: anytype,
    names: []const []const u8,
    city_idx: *const std.StringHashMap(usize),
    stats: []const Stats,
) !void {
    try writer.writeByte('{');
    for (names, 0..) |city, i| {
        if (i != 0) {
            try writer.writeByte(',');
            try writer.writeByte(' ');
        }

        const idx = city_idx.get(city).?;
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

pub fn main() !void {
    const filename = "1B.txt";

    const file = std.fs.cwd().openFile(filename, .{}) catch |e| {
        std.debug.print("{any}", .{e});
        @panic("Error: failed to open file.");
    };
    defer file.close();

    const cpu_count = try std.Thread.getCpuCount();
    var threads = try std.heap.smp_allocator.alloc(std.Thread, cpu_count);
    defer std.heap.smp_allocator.free(threads);

    var pager = try mmap.MmapPager.init(file.handle);
    defer pager.deinit();

    // create chunks naively: N/k for now
    const chunksize = pager.ptr.len / cpu_count;

    const contexts = try std.heap.smp_allocator.alloc(ThreadContext, cpu_count);
    defer std.heap.smp_allocator.free(contexts);

    var spawned: usize = 0;
    var cursor: usize = 0;
    for (0..cpu_count) |i| {
        if (cursor >= pager.ptr.len) break;

        const raw_end = cursor + chunksize;
        var end = @min(raw_end, pager.ptr.len);

        if (i == cpu_count - 1) {
            end = pager.ptr.len;
        } else {
            while (end < pager.ptr.len and pager.ptr[end] != '\n') : (end += 1) {}
            if (end < pager.ptr.len) end += 1; // go past the newline
        }

        const idx = spawned;

        contexts[idx] = .{
            .bytes = pager.ptr[cursor..end],
            .map = std.hash_map.StringHashMap(Stats).init(std.heap.smp_allocator),
        };

        threads[idx] = try std.Thread.spawn(.{}, parseRange, .{&contexts[idx]});

        spawned += 1;
        cursor = end;
    }

    for (threads[0..spawned]) |thread| {
        thread.join();
    }

    var ct: usize = 0;
    var city_idx = std.hash_map.StringHashMap(usize).init(std.heap.smp_allocator);
    defer city_idx.deinit();

    var stats = try std.ArrayList(Stats).initCapacity(std.heap.smp_allocator, 128);
    defer stats.deinit(std.heap.smp_allocator);

    var names = try std.ArrayList([]const u8).initCapacity(std.heap.smp_allocator, 128);
    defer names.deinit(std.heap.smp_allocator);

    for (contexts) |*ctx| {
        var it = ctx.map.iterator();
        while (it.next()) |e| {
            const city = e.key_ptr.*;
            const cur = e.value_ptr.*;

            if (city_idx.get(city)) |idx| {
                const old = stats.items[idx];
                stats.items[idx] = Stats{
                    .min = @min(old.min, cur.min),
                    .max = @max(old.max, cur.max),
                    .n = old.n + cur.n,
                    .sum = old.sum + cur.sum,
                };
            } else {
                try city_idx.put(city, ct);
                try names.append(std.heap.smp_allocator, city);
                try stats.append(std.heap.smp_allocator, cur);
                ct += 1;
            }
        }
    }

    std.mem.sort([]const u8, names.items, {}, stringLessThan);

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    try emitResults(stdout, names.items, &city_idx, stats.items);
    try stdout.flush();

    for (contexts[0..spawned]) |*ctx| {
        ctx.map.deinit();
    }
}

// useful to validate correctness. the "checksum" is just the sum of all the sums. it's crude but good enough.
fn checksumFile(filename: []const u8) !i64 {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();

    var pager = try mmap.MmapPager.init(file.handle);
    defer pager.deinit();

    var ctx = ThreadContext{
        .bytes = pager.ptr,
        .map = std.StringHashMap(Stats).init(allocator),
    };
    defer ctx.map.deinit();

    try parseRange(&ctx);

    var total: i64 = 0;
    var it = ctx.map.iterator();
    while (it.next()) |entry| {
        total += entry.value_ptr.sum;
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
