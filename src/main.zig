const std = @import("std");

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

    const file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();

    var read_buf: [150]u8 = undefined;
    var reader = file.reader(&read_buf);

    var write_buf: [4096]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&write_buf);
    const stdout = &writer.interface;
    defer stdout.flush() catch {};

    while (try reader.interface.takeDelimiter('\n')) |line| {
        try stdout.print("{s}\n", .{line});
    }
}
