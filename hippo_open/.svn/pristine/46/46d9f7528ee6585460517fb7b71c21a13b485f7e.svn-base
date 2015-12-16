package com.pinganfu.hippo.mdb;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockSizeMapping {
    public final Map<String, Integer> SIZE_COUNT = new HashMap<String, Integer>();
    public final Map<String, Integer> SIZE_PER = new HashMap<String, Integer>();
    public List<Double> sizeTypes = null;

    public BlockSizeMapping(List<Double> sizeTypes) {
        this.sizeTypes = sizeTypes;
        Collections.sort(sizeTypes);

        for (Double size : sizeTypes) {
            int count = (int) (MdbConstants.SIZE_LIMIT / size);
            SIZE_COUNT.put(size + "", count);
            SIZE_PER.put(size + "", (int) (MdbConstants.SIZE_LIMIT * size));
        }
    }

    public Integer getSIZE_COUNT(String size) {
        return SIZE_COUNT.get(size);
    }

    public Integer getSIZE_PER(String size) {
        return SIZE_PER.get(size);
    }

    public List<Double> getSizeTypes() {
        return sizeTypes;
    }

}
