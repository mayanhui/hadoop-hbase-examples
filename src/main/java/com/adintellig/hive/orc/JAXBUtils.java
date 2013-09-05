package com.adintellig.hive.orc;

/**
 * 'vv':{'ddl':"stat_date string,stat_hour string,ip string,logdate
 * string,method string,url string,UID string,pid string,aid int,wid int,vid
 * int,type int,stat int,mtime float,ptime float,channel string,boxver
 * string,bftime int,country string,province string,city string,isp
 * string,ditchid int,drm int,charge int,ad int,adclick int,groupid int,client
 * int,usertype int,ptolemy int,fixedid string,userid string",'mode':'hour'},
 * 
 * 
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public class JAXBUtils {

	private JAXBUtils() {
		throw new UnsupportedOperationException();
	}

	public static Object unmarshal(String contextPath, InputStream xmlStream)
			throws JAXBException {

		JAXBContext jc = JAXBContext.newInstance(contextPath);
		Unmarshaller u = jc.createUnmarshaller();

		return u.unmarshal(xmlStream);
	}

	public static void marshal(String contextPath, Object obj,
			OutputStream stream) throws JAXBException {

		JAXBContext jc = JAXBContext.newInstance(contextPath);
		Marshaller m = jc.createMarshaller();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		m.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");

		m.marshal(obj, stream);
	}

	public static void main(String[] args) throws FileNotFoundException,
			JAXBException {

		// unmarshal
//		InputStream is = new FileInputStream("/root/test.xml");
//		Object obj = JAXBUtils.unmarshal("com.adintellig.hive.orc", is);
//		System.out.println(v.getUserid());
		User u = new User();
		u.name = "xiaoming";
		u.age = 12;
		u.gender = 1;
		u.score = 0.8f;
		
		// marshal
		OutputStream f = new FileOutputStream("/root/test1.xml");
		JAXBUtils.marshal("com.adintellig.hive.orc", u, f);
	}
	
	
	static class User{
		String name;
		int age;
		int gender;
		float score;
		public User(){}
	}
}