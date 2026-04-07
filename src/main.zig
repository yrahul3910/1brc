const std = @import("std");
const mmap = @import("mmap.zig");

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

    var temp_map = std.StringHashMap(i64).init(allocator);
    defer temp_map.deinit();

    var parsing_temp = false;
    var cs: usize = 0; // where did the current city start?
    var semi: usize = 0; // where did the temp start? points to ;
    var multiplier: i64 = 1;
    var cur_temp: i64 = 0;

    // print at end to avoid compiler optimizing away all calcs
    var total_sum: i64 = 0;

    for (pager.ptr, 0..) |c, i| {
        switch (c) {
            ';' => {
                parsing_temp = true;
                semi = i;
            },
            '\n' => {
                const cur_val = temp_map.get(pager.ptr[cs..semi]) orelse 0;
                const updated = multiplier * cur_temp + cur_val;
                total_sum += updated;
                try temp_map.put(pager.ptr[cs..semi], updated);

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

    std.debug.print("{d}", .{total_sum});
}
