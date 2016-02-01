package com.ibm.gaiandb.utils;

public class Pair <Class1, Class2> {
	public final Class1 i1;
	public final Class2 i2;
	
	public Pair(Class1 i1, Class2 i2) { this.i1 = i1; this.i2 = i2; }
	
	public Class1 getFirst() { return i1; }
	public Class2 getSecond() { return i2; }
}