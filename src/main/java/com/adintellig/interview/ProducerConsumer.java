package com.adintellig.interview;

import java.util.ArrayList;
import java.util.List;

/**
 * 创建3个线程，1个启动线程（main），一个线程负责向List中添加数据，1个线程负责取出数据 用代码描述出来？
 * 
 */
public class ProducerConsumer {

	private List<String> list = new ArrayList<String>();

	public synchronized void add(String item) {
		list.add(item);
	}

	public synchronized String read(int i) {
		return list.get(i);
	}

	public static void main(String[] args) {
		ProducerConsumer pc = new ProducerConsumer();
		new Thread(new Producer(pc)).start();
		new Thread(new Consumer(pc)).start();
	}
}

class Producer extends Thread {

	ProducerConsumer pc;

	public Producer(ProducerConsumer pc) {
		this.pc = pc;
	}

	@Override
	public void run() {
		for (int i = 0; i < 1000; i++) {
			pc.add("item-" + i);
			System.out.println("Add: " + "item-" + i);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

class Consumer extends Thread {

	ProducerConsumer pc;

	public Consumer(ProducerConsumer pc) {
		this.pc = pc;
	}

	@Override
	public void run() {
		for (int i = 0; i < 1000; i++) {

			try {
				System.out.println("Read: " + pc.read(i));
			} catch (Exception e) {
				System.out.println(e.getMessage());
				try {
					Thread.sleep(500);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
