package com.adintellig.advindex;

import java.math.BigDecimal;

public class Attribute {
	private int count;
	private double sum;
	private double percent;

	public double getPercent() {
		return percent;
	}

	public void setPercent(double percent) {
		this.percent = percent;
	}

	public void add(double value) {
		this.sum += value;
		this.count += 1;
	}

	public double avg() {
		return round(this.sum / this.count);
	}
	
	public double round(double d) {
		BigDecimal b = new BigDecimal(d);
		return b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	
	public static void main(String[] args){
		double d = 9.319999694824219d;
//		d= 6.500000000000001d;
		Attribute a = new Attribute();
		a.add(d);
		System.out.println("\t"+a.avg() + "\t");
	}
	
}
