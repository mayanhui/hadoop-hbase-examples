package com.adintellig.interview;

/**
 * 
 * 对于一个已经排好序的数组a，给定一个数X，判断该数组中是否存在2个数的和等于X 要求时间复杂度为0（n）
 */
public class SortArraySum {

	public static void main(String[] args) {
		int[] a = { 1, 2, 3, 4, 5, 6 };
		int X = 8;
		int i = 0;
		int j = a.length - 1;
		int temp;
		while (i != j) {
			temp = a[i] + a[j];
			if (temp == X) {
				System.out.println("存在这样2个数 ：" + a[i] + "," + a[j]);
				break;
			} else if (temp > X) {
				j--;
			} else if (temp < X) {
				i++;
			}
			if (temp != X) {
				System.out.println("不存在这样2个数");

			}
		}
	}
}
