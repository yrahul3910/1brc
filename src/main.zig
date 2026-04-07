const std = @import("std");
const mmap = @import("mmap.zig");

const Stats = struct {
    min: i32,
    max: i32,
    sum: i64,
    n: u32,
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

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <filename>\n", .{args[0]});
        return error.MissingFilename;
    }

    const filename = args[1];

    const file = std.fs.cwd().openFile(filename, .{}) catch |e| {
        std.debug.print("{any}", .{e});
        @panic("Error: failed to open file.");
    };
    defer file.close();

    var pager = try mmap.MmapPager.init(file.handle);
    defer pager.deinit();

    var temp_map = std.StringHashMap(Stats).init(allocator);
    defer temp_map.deinit();

    var parsing_temp = false;
    var cs: usize = 0; // where did the current city start?
    var semi: usize = 0; // where did the temp start? points to ;
    var multiplier: i32 = 1;
    var cur_temp: i32 = 0;

    // print at end to avoid compiler optimizing away all calcs
    var total_sum: i64 = 0;

    for (pager.ptr, 0..) |c, i| {
        switch (c) {
            ';' => {
                parsing_temp = true;
                semi = i;
            },
            '\n' => {
                const newt = multiplier * cur_temp;
                total_sum += newt;

                try updateRecord(&temp_map, pager.ptr[cs..semi], newt);

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

    if (cs < pager.len) {
        const newt = multiplier * cur_temp;
        total_sum += newt;

        try updateRecord(&temp_map, pager.ptr[cs..semi], newt);
    }

    std.debug.print("{d}", .{total_sum});
}
