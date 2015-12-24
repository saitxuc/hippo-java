package com.hippo.redis.paser.util;

/**
 * 
 * 使用LZF算法对字符串的压缩和解压
 *
 */
public final class LZFCompress {

    /**
     * The number of entries in the hash table. The size is a trade-off between
     * hash collisions (reduced compression) and speed (amount that fits in CPU
     * cache).
     */
    private static final int HASH_SIZE = 1 << 14;

    /**
     * The maximum number of literals in a chunk (32).
     */
    private static final int MAX_LITERAL = 1 << 5;

    /**
     * The maximum offset allowed for a back-reference (8192).
     */
    private static final int MAX_OFF = 1 << 13;

    /**
     * The maximum back-reference length (264).
     */
    private static final int MAX_REF = (1 << 8) + (1 << 3);

    /**
     * Hash table for matching byte sequences (reused for performance).
     */
    private static int[] cachedHashTable;

    /**
     * Return byte with lower 2 bytes being byte at index, then index+1.
     */
    private static int first(byte[] in, int inPos) {
        return (in[inPos] << 8) | (in[inPos + 1] & 255);
    }

    /**
     * Shift v 1 byte left, add value at index inPos+2.
     */
    private static int next(int v, byte[] in, int inPos) {
        return (v << 8) | (in[inPos + 2] & 255);
    }

    /**
     * Compute the address in the hash table.
     */
    private static int hash(int h) {
        return ((h * 2777) >> 9) & (HASH_SIZE - 1);
    }

    /**
     * Compress a number of bytes.
     *
     * @param in the input data
     * @param inLen the number of bytes to compress
     * @param out the output area
     * @param outPos the offset at the output array
     * @return the end position
     */
    public static int compress(byte[] in, int inLen, byte[] out, int outPos) {
        int inPos = 0;
        if (cachedHashTable == null) {
            cachedHashTable = new int[HASH_SIZE];
        }
        int[] hashTab = cachedHashTable;
        int literals = 0;
        outPos++;
        int future = first(in, 0);
        while (inPos < inLen - 4) {
            byte p2 = in[inPos + 2];
            // next
            future = (future << 8) + (p2 & 255);
            int off = hash(future);
            int ref = hashTab[off];
            hashTab[off] = inPos;
            if (ref < inPos && ref > 0 && (off = inPos - ref - 1) < MAX_OFF && in[ref + 2] == p2 && in[ref + 1] == (byte) (future >> 8) && in[ref] == (byte) (future >> 16)) {
                // match
                int maxLen = inLen - inPos - 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    // multiple back-references,
                    // so there is no literal run control byte
                    outPos--;
                } else {
                    // set the control byte at the start of the literal run
                    // to store the number of literals
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in[ref + len] == in[inPos + len]) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                // move one byte forward to allow for a literal run control byte
                outPos++;
                inPos += len;
                // Rebuild the future, and store the last bytes to the hashtable.
                // Storing hashes of the last bytes in back-reference improves
                // the compression ratio and only reduces speed slightly.
                future = first(in, inPos);
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
            } else {
                // copy one byte from input to output as part of literal
                out[outPos++] = in[inPos++];
                literals++;
                // at the end of this literal chunk, write the length
                // to the control byte and start a new chunk
                if (literals == MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    // move ahead one byte to allow for the
                    // literal run control byte
                    outPos++;
                }
            }
        }
        // write the remaining few bytes as literals
        while (inPos < inLen) {
            out[outPos++] = in[inPos++];
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }
        // writes the final literal run length to the control byte
        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

    /**
     * Expand a number of compressed bytes.
     *
     * @param in the compressed data
     * @param inPos the offset at the input array
     * @param inLen the number of bytes to read
     * @param out the output area
     * @param outPos the offset at the output array
     * @param outLen the size of the uncompressed data
     */
    public static void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) {
        if (inPos < 0 || outPos < 0 || outLen < 0) {
            throw new IllegalArgumentException();
        }
        do {
            int ctrl = in[inPos++] & 255;
            if (ctrl < MAX_LITERAL) {
                // literal run of length = ctrl + 1,
                ctrl++;
                // copy to output and move forward this many bytes
                System.arraycopy(in, inPos, out, outPos, ctrl);
                outPos += ctrl;
                inPos += ctrl;
            } else {
                // back reference
                // the highest 3 bits are the match length
                int len = ctrl >> 5;
                // if the length is maxed, add the next byte to the length
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                // minimum back-reference is 3 bytes,
                // so 2 was subtracted before storing size
                len += 2;

                // ctrl is now the offset for a back-reference...
                // the logical AND operation removes the length bits
                ctrl = -((ctrl & 0x1f) << 8) - 1;

                // the next byte augments/increases the offset
                ctrl -= in[inPos++] & 255;

                // copy the back-reference bytes from the given
                // location in output to current position
                ctrl += outPos;
                if (outPos + len > outLen) {
                    // reduce array bounds checking
                    throw new ArrayIndexOutOfBoundsException();
                }
                for (int i = 0; i < len; i++) {
                    out[outPos++] = out[ctrl++];
                }
            }
        } while (outPos < outLen);
    }

}