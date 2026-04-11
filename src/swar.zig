const std = @import("std");

// ported from: https://github.com/dans-stuff/swar
pub fn find(text: []const u8, byte: u8) ?usize {
    var i: usize = 0;
    const byte_u64: u64 = @intCast(byte);
    // broadcast target byte
    const duped = byte_u64 * 0x0101_0101_0101_0101;

    while (i + 8 <= text.len) {
        const x0 = std.mem.readInt(u64, text[i..][0..8], .little);

        // turn matches to zeros
        const x = x0 ^ duped;

        // SIMD within a register (SWAR), aka packed SIMD
        // swar: zero-byte detection trick:
        // for each byte:
        // 1. mask off bit 7 (the first &) so the addition doesn't carry to the next byte
        // 2. adding that value sets bit 7 for any nonzero byte, so a zero byte stays below 0x80
        // 3. OR with the original x to also catch bytes with bit 7 already set
        // now, bit 7 of `y` is 1 for every nonzero byte
        const y = ((x & 0x7F7F7F7F7F7F7F7F) + 0x7F7F7F7F7F7F7F7F) | x;

        // invert and mask so bit 7 is set only where the original byte was 0 (i.e., target matched)
        const hi = ~y & 0x8080_8080_8080_8080;
        if (hi != 0) return i + @ctz(hi) / 8;

        i += 8;
    }

    while (i < text.len) : (i += 1) {
        if (text[i] == byte) return i;
    }

    return null;
}

pub fn findSIMD(text: []const u8, byte: u8) ?usize {
    var i: usize = 0;
    const vec_size = 16;
    const duped = @as(@Vector(vec_size, u8), @splat(byte));

    while (i + vec_size <= text.len) {
        const x0 = @as(@Vector(vec_size, u8), text[i..][0..vec_size].*);
        const matches = x0 == duped;
        const mask = @as(u16, @bitCast(matches));

        if (mask != 0) return i + @ctz(mask);

        i += vec_size;
    }

    while (i < text.len) : (i += 1) {
        if (text[i] == byte) return i;
    }

    return null;
}
