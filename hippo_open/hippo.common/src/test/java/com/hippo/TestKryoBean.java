package com.hippo;

import com.esotericsoftware.kryo.serializers.FieldSerializer.Optional;

/**
 * 
 * @author saitxuc
 *
 */
public class TestKryoBean implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2248208346778901271L;
	
	private String att1;
	
	@Optional(value = "")
	private byte[] att2;
	
	public TestKryoBean() {
		
	}

	public String getAtt1() {
		return att1;
	}

	public void setAtt1(String att1) {
		this.att1 = att1;
	}

	public byte[] getAtt2() {
		return att2;
	}

	public void setAtt2(byte[] att2) {
		this.att2 = att2;
	}

	
	
	
	
}
