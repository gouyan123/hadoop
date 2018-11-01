package com.gouyan.lucene.analizer;

import java.util.Random;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

public class MyAttribute1Impl extends AttributeImpl {

	int value = 0;
	Random rd = new Random();

	public int getAttr() {
		return value;
	}

	@Override
	public void clear() {
		value = rd.nextInt(1000);
	}

	@Override
	public void reflectWith(AttributeReflector reflector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void copyTo(AttributeImpl target) {

	}
}
