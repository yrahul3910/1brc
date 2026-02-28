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

    std.debug.print("mmap ok, size={d}\n", .{pager.len});
}
