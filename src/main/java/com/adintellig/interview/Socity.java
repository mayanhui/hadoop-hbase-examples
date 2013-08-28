package com.adintellig.interview;

/**
 * 某国家的家庭都喜欢生男孩，而且一个家庭直到生有男孩为止，否则继续生小孩， 
 * 该国当前的男女均衡，问若干年后该国家的男女比例的趋势？
 * 
 * 就这道题目来说，可以先考察特殊情况。 
 * 一对夫妻在三年后的情况 一共有四种 
 * 第一种: 第一年生男孩 男孩的比例为1 出现的概率为1/2 
 * 第二种: 第二年生男孩 男孩的比例为1/2 出现的概率为1/4 
 * 第三种: 第三年生男孩 男孩的比例为1/3 出现的概率为1/8 
 * 第四种: 三年都是女孩 男孩的比例为0 出现的概率为1/8 
 * 则第三年这对夫妻子女中，男孩比例的期望值为
 * 1*(1/2)+(1/2)*(1/4)+(1/3)*(1/8)+0*(1/8)
 * 
 * 推广到一般情况，n年后一对夫妻子女中的男孩比例的期望值为：Σ1/（i*2^(i)），这个值显然大于1/2
 * 既然每对夫妻子女中男孩比例的期望值都大于1/2，那么国家必然是男孩多的局面。
 */
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
/*
 * 社会类
 * 一个社会由多个夫妻组成
 * 这个社会只关心人口比例（悲剧）
 */
public class Socity {
	private List<Couple> list;//夫妻
	private int m;//男孩总数
	private int f;//女孩总数
	
	public Socity(int n){
		//初始化社会，指定这个社会有多少夫妻
		this.list=new ArrayList<Couple>();
		for(int i=0;i<n;i++){
			list.add(new Couple());
		}
	}
	
	/**
	 * 计算下n年后男孩的比例
	 * 
	 * 
	 */
	
	public float getRateOfGender(int n){
		int males=0;
		int famales=0;
		for(int i=0;i<this.list.size();i++){
			list.get(i).born(n);
			males+=list.get(i).getmN();
			famales+=list.get(i).getfN();
		}
		m=males;
		f=famales;
		
		return (float)males/(famales+males);
	}
	
	/**
	 * 夫妻类，这些夫妻只会生孩子（再次悲剧），他们的子女就别生了吧
	 * 
	 *
	 */
	
	class Couple{
		private int mN;//男孩数量
		private int fN;//女孩数量
		
		/**
		 * 
		 * n年了，光生孩子了
		 */
		void born(int n){
			int tempM=0;
			int tempF=0;
			Random random=new Random();
			boolean stop=false;
			for(int i=0;i<n;i++){
				if(stop){
					break;
				}
				if(random.nextInt(2)==1){
					tempM++;
					stop=true;//总算生到男孩了，可以不用生了
				}else{
					tempF++;
				}
			}
			mN=tempM;
			fN=tempF;
		}
 
		public int getmN() {
			return mN;
		}
 
		public int getfN() {
			return fN;
		}
	}
	
	public List<Couple> getList() {
		return list;
	}
 
	public int getM() {
		return m;
	}
 
	public int getF() {
		return f;
	}
	
	/**
	 * 做个试验吧
	 * 据说社会学是不能做实验的，虚拟世界里，人也成了小白鼠
	 * 就用10000对夫妻做100次试验吧
	 * 看看5年后男孩的人口比例到底有多少
	 */
 
	public static void main(String[] args) {
		Socity socity = new Socity(10000);
		float sum = 0;
		float average = 0;
		for (int i = 0; i < 100; i++) {
			System.out.println("第" + (i + 1) + "次试验");
			float rate = socity.getRateOfGender(10);
			System.out.println("男性比例为" + rate + " 男性 " + socity.getM() + " 女性"
					+ socity.getF());
			sum += rate;
			System.out.println();
			System.out.println();
		}
		average = sum / 100;
		System.out.println("100次试验中男性平均比例" + average);
	}
 
}
