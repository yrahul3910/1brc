const std = @import("std");
const tracy = @import("tracy");

pub const MmapPager = struct {
    ptr: []align(std.heap.page_size_min) u8,
    len: u64,

    pub fn init(fd: std.posix.fd_t) !MmapPager {
        const stat = try std.posix.fstat(fd);

        // TODO: Update flags for other OS
        const ptr = std.posix.mmap(
            null,
            @intCast(stat.size),
            std.posix.PROT.READ,
            std.posix.MAP{ .TYPE = .PRIVATE },
            fd,
            0,
        ) catch |e| {
            std.debug.print("Error: {any}", .{e});
            @panic("Failed to mmap");
        };
        try std.posix.madvise(@ptrCast(ptr), @intCast(stat.size), std.posix.MADV.SEQUENTIAL);

        return .{ .ptr = ptr, .len = @intCast(stat.size) };
    }

    pub fn deinit(self: *MmapPager) void {
        std.posix.munmap(self.ptr);
        self.ptr = undefined;
        self.len = 0;
    }
};
