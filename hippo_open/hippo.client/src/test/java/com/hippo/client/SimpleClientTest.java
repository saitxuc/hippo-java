package com.hippo.client;

import com.hippo.client.impl.HippoClientImpl;
import com.hippo.common.util.DataUtil;

public class SimpleClientTest {
    public static void main(String[] args) {
        String cclusterName = "ccluster-open-mdb";
        String zk = "192.168.1.98:2181,192.168.1.99:2181,192.168.1.100:2181";

        HippoConnector hippoConnector = new HippoConnector();
        hippoConnector.setClusterName(cclusterName);
        hippoConnector.setZookeeperUrl(zk);
        final HippoClientImpl client = new HippoClientImpl(hippoConnector);
        client.start();

        HippoResult result = client.getWholeBit("testbitset10", 10000050, 10, 60);
        if (result.isSuccess()) {
            byte[] data = result.getData();
            for (int i = 0; i < data.length; i++) {
                if (data[i] != -1) {
                    System.out.println(i + " --- " + data[i]);
                }
            }
            System.out.println(result.getDataForObject(Long.class));
        }
       /* HippoResult result = client.getBit("testbitset1", 3927124, 10);
        if (result.isSuccess()) {
            byte[] re = result.getData();
            if (DataUtil.getBoolean(re[0])) {
            } else {
                System.out.println("not right!!");
            }
        } else {
            System.out.println("error code : ->" + result.getErrorCode());
        }*/
        client.stop();
    }
}
