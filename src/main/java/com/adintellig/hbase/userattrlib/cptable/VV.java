package com.adintellig.hbase.userattrlib.cptable;

import java.io.IOException;
import java.lang.Character.UnicodeBlock;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.adintellig.util.UnicodeTransverter;

public class VV {
	private String vid = "";
	private String aid = "";
	private String uid = "";
	private long time;
	private String ver = "";
	private long timecount = 300;
	private Video video = new Video();

	public String getVid() {
		return vid;
	}

	public void setVid(String vid) {
		this.vid = vid;
	}

	public String getAid() {
		return aid;
	}

	public void setAid(String aid) {
		this.aid = aid;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public long getTimecount() {
		return timecount;
	}

	public void setTimecount(long timecount) {
		this.timecount = timecount;
	}

	public Video getVideo() {
		return video;
	}

	public void setVideo(Video video) {
		this.video = video;
	}
	
	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}
	
	public String getVer() {
		return ver;
	}

	public void setVer(String ver) {
		this.ver = ver;
	}

	public static void main(String[] args) throws JsonGenerationException,
			JsonMappingException, IOException {

		ObjectMapper mapper = new ObjectMapper();
		long st = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			VV vv = new VV();
			vv.setTime(137000125688L);
			vv.setAid("123123");
			Video video = vv.getVideo();
			video.setAid("12321");
			video.setVtitle("曼谷杀手");
			video.setMovieid("MID123");
			video.setWid("WID34");
//			video.setVtitle("NAME-NAME");

			String json = mapper.writeValueAsString(vv);
			System.out.println(json);
			json = UnicodeTransverter.utf8ToUnicode(json);
			System.out.println(json);
			
			System.out.println(UnicodeTransverter.unicodeToUtf8("\u6697\u604b99\u5929"));
		}
		
		long en = System.currentTimeMillis();
		System.out.println(en - st);

//		for (int i = 0; i < 1000; i++) {
//			String aid = "123123";
//			long ts = 137000125688L;
//			String title = "NAME-NAME";
//			String mid = "MID123";
//			String wid = "WID34";
//			String json = "{\"vid\":\"\",\"aid\":\""
//					+ aid
//					+ "\",\"time\":"
//					+ ts
//					+ ",\"timecount\":0,\"video\":{\"channel\":\"\",\"vid\":\"\",\"aid\":\""
//					+ aid + "\",\"vtitle\":\"" + title + "\",\"wid\":\"" + wid
//					+ "\",\"pid\":\"\",\"movieid\":\"" + mid + "\"}}";
//
//			// System.out.println(json);
//		}
//		long en2 = System.currentTimeMillis();
//		System.out.println(en2 - en);
		
//		String line = "\\\\x5Cu670b\\\\x5Cu53cb\\\\x5Cu4e5f\\\\x5Cu4e0a\\\\x5Cu5e8a: \\\\x5Cu7b2c1\\\\x5Cu5b63";
//		line = unicodeToUtf8(line);
//		System.out.println(line);
	}
	
}
