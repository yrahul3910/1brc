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

pub fn main() !void {
    const filename = "10M.txt";

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
