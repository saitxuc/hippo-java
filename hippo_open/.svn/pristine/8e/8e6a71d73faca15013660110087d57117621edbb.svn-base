package com.pinganfu.hippo.mdb.utils;

import java.util.List;

import com.pinganfu.hippo.mdb.BlockSizeMapping;
import com.pinganfu.hippo.mdb.MdbConstants;

/**
 * 
 * @author saitxuc
 */
public class BufferUtil {
    /***
    private static final int SEPARATOR_LENGTH;
    private static final byte[] SEPARATOR;
    private static final Serializer serializer = new KryoSerializer();
    static {
        try {
            SEPARATOR = serializer.serialize(":");
            SEPARATOR_LENGTH = SEPARATOR.length;
         } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
    ***/
    public static byte[] composite(byte[] header, byte[] value, byte[] tail, byte[] separator) {
        try {
            final int hl = header.length, tl = tail.length, vl = value.length;
            byte[] composite = new byte[separator.length * 2 + hl + tl + vl];
            System.arraycopy(header, 0, composite, 0, hl);
            System.arraycopy(separator, 0, composite, hl, separator.length);
            System.arraycopy(value, 0, composite, hl + separator.length, vl);
            System.arraycopy(separator, 0, composite, hl + separator.length + vl, separator.length);
            System.arraycopy(tail, 0, composite, hl + (separator.length * 2) + vl, tl);
            return composite;
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public static byte[] separate(final byte[] source, int klength, int separatorLength, int expireLength) {
        int index = separatorLength + klength + MdbConstants.HEADER_LENGTH_FOR_INT * 3;
        int dlength = source.length - index - separatorLength - expireLength;
        final byte[] target = new byte[dlength];
        System.arraycopy(source, index, target, 0, dlength);
        return target;
    }

    public static byte[] separateKey(final byte[] source, int index, int klength) {
        int dLength = klength;
        final byte[] target = new byte[dLength];
        System.arraycopy(source, index, target, 0, dLength);
        return target;
    }

    public static String getSizePeriod(int length, BlockSizeMapping sizeMapping) {
        List<Double> sizes = sizeMapping.getSizeTypes();

        for (int index = 0; index < sizes.size(); index++) {
            String size = sizes.get(index) + "";
            if (length > sizeMapping.getSIZE_PER(size)) {
                continue;
            } else {
                return size;
            }
        }

        return null;
    }
}
