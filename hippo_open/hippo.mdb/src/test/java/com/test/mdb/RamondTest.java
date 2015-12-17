package com.test.mdb;

import java.util.Random;

public class RamondTest {

	public static void main(String[] args) {
		
		for(int i = 0; i< 200; i++) {
			Random random =new Random();
			String key1 = "k" + random.nextInt(1);
			System.out.println("-----key1------->"+key1);
		}
		
		
	}

}
