package com.pinganfu.hippo.redis.paser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pinganfu.hippo.common.store.ReZSetEntry;
import com.pinganfu.hippo.common.store.ZSetEntryComparator;
import com.pinganfu.hippo.redis.paser.util.IntSet;
import com.pinganfu.hippo.redis.paser.util.LZFCompress;
import com.pinganfu.hippo.redis.paser.util.RedisNumUtil;
import com.pinganfu.hippo.redis.paser.util.ZipList;
import com.pinganfu.hippo.redis.paser.util.ZipMap;

public class RDBPaser {
    private static final Logger LOG = LoggerFactory.getLogger(RDBPaser.class);
    
    private FileChannel channel;

    private ByteBuffer bytebuffer;

    private FileInputStream fileStream;

    public RDBPaser() {
    }

    public void init(File file) {
        LOG.info("begin to init the RDBPaser......");

        try {
            fileStream = new FileInputStream(file);
            channel = fileStream.getChannel();
            bytebuffer = ByteBuffer.allocateDirect(RedisConstants.BUFFER_SIZE);
            bytebuffer.position(0);
            bytebuffer.limit(0);
        } catch (FileNotFoundException e) {
            runtimeError("get file input stream failed!!");
        }

        LOG.info("RDBPaser init finish!");
    }

    private void runtimeError(String msg, Object... args) {
        throw new RuntimeException(String.format(msg, args));
    }

    public boolean verifyMagicString() {
        byte[] data = new byte[5];
        boolean status = readBytes(data, 0, 5);
        if (status) {
            //0~5组合必须为REDIS
            if ("REDIS".equals(new String(data))) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public boolean verifyVersion() {
        byte[] data = new byte[4];
        boolean status = readBytes(data, 0, 4);
        if (status) {
            //The 4 bytes are interpreted as ascii characters and then converted to an integer using string to integer conversion.
            String version = null;
            try {
                version = new String(data, "ASCII");
            } catch (UnsupportedEncodingException e) {
                runtimeError("Invalid RDB version number");
            }

            int ver = Integer.parseInt(version);
            if (ver < 1 || ver > 6) {
                runtimeError("Invalid RDB version number %d", ver);
                return false;
            } else {
                LOG.info("the rdb version for redis is " + ver);
                return true;
            }
        } else {
            return false;
        }
    }

    private boolean readBytes(byte[] buf, int startIndex, int byteNum) {
        if (byteNum <= 0) {
            runtimeError("letter number must bigger than 0!!");
        }

        int effectNum = Math.min(bytebuffer.remaining(), byteNum);
        bytebuffer.get(buf, startIndex, effectNum);

        //could not reach the effective number and need to get more byte
        while (effectNum < byteNum) {
            startIndex = startIndex + effectNum;
            byteNum = byteNum - effectNum;
            bytebuffer.clear();

            int read = 0;
            while (read == 0) {
                try {
                    read = channel.read(bytebuffer);
                } catch (Exception e) {
                    return false;
                }
                if (read == -1) {
                    return false;
                }
            }

            bytebuffer.position(0);
            bytebuffer.limit(read);

            effectNum = Math.min(bytebuffer.remaining(), byteNum);

            bytebuffer.get(buf, startIndex, effectNum);
        }
        return true;
    }

    private long ntohl() {
        byte[] bytes = new byte[4];
        long val = 0;
        if (readBytes(bytes, 0, 4)) {
            val = ((long) bytes[0] & 0xFF) | ((long) (bytes[1] & 0xFF) << 8) | ((long) (bytes[2] & 0xFF) << 16) | ((long) (bytes[3] & 0xFF) << 24);
        } else {
            return Long.MAX_VALUE;
        }

        long new_val = 0;
        new_val = new_val | ((val & 0x000000ff) << 24);
        new_val = new_val | ((val & 0xff000000) >> 24);
        new_val = new_val | ((val & 0x0000ff00) << 8);
        new_val = new_val | ((val & 0x00ff0000) >> 8);
        return (int) new_val;
    }

    private long getDataType() {
        byte[] data_type = new byte[1];
        if (readBytes(data_type, 0, 1)) {
            return 0xff & data_type[0];
        } else {
            return Long.MAX_VALUE;
        }
    }

    public RedisEntry loadEntry() {
        RedisEntry entry = new RedisEntry();

        while (true) {
            int dataType = (int) getDataType();

            if (Long.MAX_VALUE == dataType) {
                runtimeError("could not the dataType");
            }

            if (dataType == RedisConstants.REDIS_RDB_OPCODE_EXPIRETIME_MS) {
                byte[] timeBytes = new byte[8];
                readBytes(timeBytes, 0, 8);
                entry.setExpire(RedisNumUtil.getLong(timeBytes, 0));
                dataType = (int) getDataType();
                if (dataType == Long.MAX_VALUE) {
                    runtimeError("could not get the data type");
                }
                entry.setType(dataType);
            } else if (dataType == RedisConstants.REDIS_RDB_OPCODE_EXPIRETIME) {
                byte[] timeBytes = new byte[4];
                readBytes(timeBytes, 0, 4);
                entry.setExpire(RedisNumUtil.bytesToInt(timeBytes, 0) * 1000);
                dataType = (int) getDataType();
                if (dataType == Long.MAX_VALUE) {
                    runtimeError("could not get the data type");
                }
                entry.setType(dataType);
            }

            if (dataType == RedisConstants.REDIS_RDB_OPCODE_SELECTDB) {
                //db changed
                int db_number = (int) getDataType();
                if (db_number == Long.MAX_VALUE) {
                    runtimeError("could not get the rdb's db number!!");
                } else {
                    LOG.info("the rbd's db number is " + db_number);
                }

                entry.setType(db_number);
                continue;
            } else if (dataType == RedisConstants.REDIS_RDB_OPCODE_EOF) {
                //file end
                entry.setType(dataType);
                return entry;
            }

            entry.setType(dataType);
            if (dataType == RedisConstants.REDIS_RDB_OPCODE_SELECTDB || dataType == RedisConstants.REDIS_RDB_OPCODE_EOF || dataType == RedisConstants.REDIS_RDB_OPCODE_EXPIRETIME || dataType == RedisConstants.REDIS_RDB_OPCODE_EXPIRETIME_MS) {
                //$value_type guaranteed != to FD, FC, FE and FF
                runtimeError("dataType should not be FD, FC, FE or FF, fetch the data is %d ", dataType);
                return null;
            }

            CodedType codeType = loadLengthWithEncoding();
            if (codeType.getLength() == Long.MAX_VALUE) {
                runtimeError("load length with encoding error!!");
            }

            String key = loadKeyString(codeType);

            if (StringUtils.isEmpty(key)) {
                runtimeError("load key error!!");
            }

            entry.setKey(key);

            if (!RedisConstants.DATA_TYPE_MAPPING.containsKey(entry.getType())) {
                loadStringBytes();
                continue;
            }

            loadValue(entry);

            return entry;
        }
    }

    private void loadValue(RedisEntry entry) {
        //get the collection size for the value
        long length = 0;
        switch (entry.getType()) {
            case RedisConstants.REDIS_RDB_TYPE_LIST:
            case RedisConstants.REDIS_RDB_TYPE_SET:
            case RedisConstants.REDIS_RDB_TYPE_ZSET:
            case RedisConstants.REDIS_RDB_TYPE_HASH:
                length = loadLengthWithEncoding().getLength();
                if (length == Long.MAX_VALUE) {
                    runtimeError("could not get the value size for %d ", entry.getType());
                }
                break;
        }

        switch (entry.getType()) {
            case RedisConstants.REDIS_RDB_TYPE_STRING:
                String value = loadString();
                if (value == null) {
                    runtimeError("Error reading entry value");
                } else {
                    entry.setValue(value);
                }
                break;
            case RedisConstants.REDIS_RDB_TYPE_HASH_ZIPMAP:
                byte[] hashZipValue = loadStringBytes();
                HashMap<String, String> valueMap = ZipMap.zipmapExpand(hashZipValue);
                if (valueMap == null) {
                    runtimeError("Error reading entry value");
                }
                entry.setValue(valueMap);
                break;
            case RedisConstants.REDIS_RDB_TYPE_LIST_ZIPLIST:
                List<Object> lists = new ArrayList<Object>();
                ZipList zipList = new ZipList(loadStringBytes());
                int entryCountList = zipList.decodeEntryCount();

                for (int j = 0; j < entryCountList; j++) {
                    // value
                    lists.add(zipList.decodeEntryValue());
                }

                if (zipList.getEndByte() != 255) { // 0xff为ziplist的结束符
                    runtimeError("REDIS_RDB_TYPE_LIST_ZIPLIST | Invalid zip list end - %d for key %s", zipList.getEndByte(), entry.getKey());
                } else {
                    entry.setValue(lists);
                }
                break;
            case RedisConstants.REDIS_RDB_TYPE_SET_INTSET:
                IntSet intset = new IntSet(loadStringBytes());
                List<Long> intSetValues = intset.docodeIntsetValue();
                entry.setValue(intSetValues);
                break;
            case RedisConstants.REDIS_RDB_TYPE_ZSET_ZIPLIST:
                TreeSet<ReZSetEntry> zSetListResult = new TreeSet<ReZSetEntry>(new ZSetEntryComparator());

                ZipList zipLitForZset = new ZipList(loadStringBytes());

                int numEntriesZipList = zipLitForZset.decodeEntryCount();

                if (numEntriesZipList % 2 != 0) {
                    runtimeError("REDIS_RDB_TYPE_ZSET_ZIPLIST | Expected even number of elements, but found %d for key %s", numEntriesZipList, entry.getKey());
                }

                for (int i = 0; i < numEntriesZipList / 2; i++) {
                    Object val = zipLitForZset.decodeEntryValue();
                    Object score = zipLitForZset.decodeEntryValue();
                    zSetListResult.add(new ReZSetEntry(val, score));
                }

                if (zipLitForZset.getEndByte() != 255) {
                    runtimeError("REDIS_RDB_TYPE_ZSET_ZIPLIST | Invalid zip list end - %d for key %s", zipLitForZset.getEndByte(), entry.getKey());
                } else {
                    entry.setValue(zSetListResult);
                    zSetListResult = null;
                }
                break;
            case RedisConstants.REDIS_RDB_TYPE_HASH_ZIPLIST:
                HashMap<Object, Object> hashmapValues = new HashMap<Object, Object>();

                ZipList zipLit = new ZipList(loadStringBytes());

                int entryCount = zipLit.decodeEntryCount();

                if (entryCount % 2 != 0) {
                    runtimeError("read_hash_from_ziplist | Expected even number of elements, but found %d for key %s", entryCount, entry.getKey());
                }

                for (int j = 0; j < entryCount / 2; j++) {
                    Object hashKey = zipLit.decodeEntryValue();
                    Object hashValue = zipLit.decodeEntryValue();
                    hashmapValues.put(hashKey, hashValue);
                }

                if (zipLit.getEndByte() != 255) {
                    runtimeError("REDIS_RDB_TYPE_HASH_ZIPLIST | Invalid zip list end - %d for key %s", zipLit.getEndByte(), entry.getKey());
                } else {
                    entry.setValue(hashmapValues);
                    zipLit = null;
                }
                break;
            case RedisConstants.REDIS_RDB_TYPE_LIST:
                List<String> listValues = new ArrayList<String>();
                for (int i = 0; i < length; i++) {
                    String val = loadString();
                    if (val == null) {
                        runtimeError("Error reading element at index %d (length: %d)", i, length);
                    }
                    listValues.add(val);
                }
                entry.setValue(listValues);
                break;
            case RedisConstants.REDIS_RDB_TYPE_SET:
                HashSet<String> setValues = new HashSet<String>();
                for (int i = 0; i < length; i++) {
                    String val = loadString();
                    if (val == null) {
                        runtimeError("Error reading element at index %d (length: %d)", i, length);
                    }
                    setValues.add(val);
                }
                entry.setValue(setValues);
                break;
            case RedisConstants.REDIS_RDB_TYPE_ZSET:
                TreeSet<ReZSetEntry> zset = new TreeSet<ReZSetEntry>(new ZSetEntryComparator());
                for (int i = 0; i < length; i++) {
                    String val = loadString();
                    if (val == null) {
                        runtimeError("Error reading element key at index %d (length: %d)", i, length);
                    }
                    Double score = loadDoubleValue();
                    if (score == null) {
                        runtimeError("Error reading element value at index %d (length: %d)", i, length);
                    }

                    zset.add(new ReZSetEntry(val, score));
                }
                entry.setValue(zset);
                break;
            case RedisConstants.REDIS_RDB_TYPE_HASH:
                HashMap<String, String> mapValues = new HashMap<String, String>();
                for (int i = 0; i < length; i++) {
                    String k = loadString();
                    if (k == null) {
                        runtimeError("Error reading element key at index %d (length: %d)", i, length);
                    }
                    String val = loadString();
                    if (val == null) {
                        runtimeError("Error reading element value at index %d (length: %d)", i, length);
                    }
                    mapValues.put(k, val);
                }
                entry.setValue(mapValues);
                break;
            default:
                runtimeError("Type not implemented");
        }
    }

    private String loadKeyString(CodedType type) {
        if (type.isCoded()) {
            long val = 0;
            byte[] data = new byte[4];
            if (type.getLength() == RedisConstants.REDIS_RDB_ENC_INT8) {
                if (!readBytes(data, 0, 1)) {
                    return null;
                }
                val = data[0] & 0xff;
            } else if (type.getLength() == RedisConstants.REDIS_RDB_ENC_INT16) {
                if (!readBytes(data, 0, 2)) {
                    return null;
                }
                val = ((data[1] & 0xff) << 8) | (data[0] & 0xff);
            } else if (type.getLength() == RedisConstants.REDIS_RDB_ENC_INT32) {
                if (!readBytes(data, 0, 4)) {
                    return null;
                }
                val = RedisNumUtil.bytesToInt(data, 0);
            } else if (type.getLength() == RedisConstants.REDIS_RDB_ENC_LZF) {
                return loadLzfString();
            }
            return val + "";
        } else {
            byte[] buf = new byte[(int) type.getLength()];
            if (!readBytes(buf, 0, (int) type.getLength())) {
                return null;
            } else {
                return new String(buf);
            }
        }
    }

    private CodedType loadLengthWithEncoding() {
        CodedType type = new CodedType();

        long length = 0;
        boolean is_encoded = false;
        byte[] bytes = new byte[2];

        if (!readBytes(bytes, 0, 1)) {
            type.setLength(Long.MAX_VALUE);
            return type;
        }

        int encType = (bytes[0] & 0xC0) >> 6;
        if (encType == RedisConstants.REDIS_RDB_ENCVAL) {
            is_encoded = true;
            length = bytes[0] & 0x3F;
        } else if (encType == RedisConstants.REDIS_RDB_6BITLEN) {
            length = bytes[0] & 0x3F;
        } else if (encType == RedisConstants.REDIS_RDB_14BITLEN) {
            readBytes(bytes, 1, 1);
            length = ((bytes[0] & 0x3F) << 8) | (bytes[1] & 0xff);
        } else {
            length = ntohl();
            if (length == Long.MAX_VALUE) {
                runtimeError("loadLengthWithEncoding | could not get the length!!");
            }
        }

        type.setLength(length);
        type.setCoded(is_encoded);

        return type;
    }

    private byte[] loadLzfBytes() {
        int clen = (int) loadLengthWithEncoding().getLength();
        int slen = (int) loadLengthWithEncoding().getLength();

        if (clen == Long.MAX_VALUE)
            return null;

        if (slen == Long.MAX_VALUE)
            return null;

        byte[] c = new byte[clen];

        if (!readBytes(c, 0, clen)) {
            return null;
        }

        byte[] s = new byte[slen];

        LZFCompress.expand(c, 0, clen, s, 0, slen);

        return s;
    }

    private String loadLzfString() {
        byte[] s = loadLzfBytes();

        if (s == null) {
            return null;
        }

        try {
            return new String(s, "ASCII");
        } catch (UnsupportedEncodingException e) {
            return new String(s);
        }
    }

    private byte[] loadStringBytes() {
        CodedType type = loadLengthWithEncoding();
        if (type.isCoded()) {
            switch ((int) type.getLength()) {
                case RedisConstants.REDIS_RDB_ENC_LZF:
                    return loadLzfBytes();
                default:
                    /* unknown encoding */
                    runtimeError("Unknown string encoding (0x%02x)", type.getLength());
                    return null;
            }
        }

        if (type.getLength() == Long.MAX_VALUE)
            return null;

        byte[] buf = new byte[(int) type.getLength()];
        if (!readBytes(buf, 0, (int) type.getLength())) {
            return null;
        }
        return buf;
    }

    //load String
    private String loadString() {
        CodedType type = loadLengthWithEncoding();
        if (type.isCoded()) {
            switch ((int) type.getLength()) {
                case RedisConstants.REDIS_RDB_ENC_INT8:
                case RedisConstants.REDIS_RDB_ENC_INT16:
                case RedisConstants.REDIS_RDB_ENC_INT32:
                    return loadInteger((int) type.getLength()) + "";
                case RedisConstants.REDIS_RDB_ENC_LZF:
                    return loadLzfString();
                default:
                    /* unknown encoding */
                    runtimeError("Unknown string encoding (0x%02x)", type.getLength());
                    return null;
            }
        }

        if (type.getLength() == Long.MAX_VALUE)
            return null;

        byte[] buf = new byte[(int) type.getLength()];
        if (!readBytes(buf, 0, (int) type.getLength())) {
            return null;
        }

        return new String(buf);
    }

    //load Integer
    private Integer loadInteger(int enctype) {
        byte[] enc = new byte[4];
        Integer val;
        if (enctype == RedisConstants.REDIS_RDB_ENC_INT8) {
            if (!readBytes(enc, 0, 1))
                return null;
            val = (0xff & enc[0]);
        } else if (enctype == RedisConstants.REDIS_RDB_ENC_INT16) {
            if (!readBytes(enc, 0, 2))
                return null;
            val = ((enc[1] & 0xff) << 8) | (enc[0] & 0xff);
        } else if (enctype == RedisConstants.REDIS_RDB_ENC_INT32) {
            if (!readBytes(enc, 0, 4))
                return null;
            val = ((enc[3] & 0xff) << 24) + ((enc[2] & 0xff) << 16) + ((enc[1] & 0xff) << 8) + ((enc[0] & 0xff));
        } else {
            runtimeError("Unknown integer encoding (0x%02x)", enctype);
            return null;
        }
        return val;
    }

    /* double数据 **/
    private Double loadDoubleValue() {
        byte[] buf = new byte[256];
        byte[] lenArray = new byte[1];
        Double val;

        if (!readBytes(lenArray, 0, 1))
            return null;

        int len = (0xff & lenArray[0]);
        switch (len) {
            case 255:
                val = Double.NEGATIVE_INFINITY;
                return val;
            case 254:
                val = Double.POSITIVE_INFINITY;
                return val;
            case 253:
                val = Double.NaN;
                return val;
            default:
                if (!readBytes(buf, 0, len)) {
                    return null;
                }
                buf[len] = '\0';
                String str = "";
                try {
                    str = new String(buf, 0, len, "ASCII");
                } catch (UnsupportedEncodingException e) {
                    str = new String(buf, 0, len);
                }
                return Double.parseDouble(str);
        }
    }

    public void close() {
        if (bytebuffer != null) {
            bytebuffer.clear();
            if (bytebuffer.isDirect()) {
                ((sun.nio.ch.DirectBuffer) bytebuffer).cleaner().clean();
            }
        }

        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (fileStream != null) {
            try {
                fileStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
