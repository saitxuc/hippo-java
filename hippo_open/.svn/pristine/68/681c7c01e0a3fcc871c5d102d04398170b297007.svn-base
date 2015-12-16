package com.pinganfu.hippo;

import com.pinganfu.hippo.common.serializer.KryoSerializer;

/**
 * 
 * @author saitxuc
 *
 */
public class TestKryoOption {
	
	
	public static void main(final String[] args) throws Exception {
		TestKryoBean bean1 = new TestKryoBean();
		bean1.setAtt1("abc");
		bean1.setAtt2("def111".getBytes());
		
		KryoSerializer kryoSerializer = new KryoSerializer();
		byte[] bb = kryoSerializer.serialize(bean1);
		
		TestKryoBean bean2 = kryoSerializer.deserialize(bb, TestKryoBean.class);
		
		
	}
	
}
