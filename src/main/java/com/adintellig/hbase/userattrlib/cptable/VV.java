package com.adintellig.hbase.userattrlib.cptable;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class VV {
	private String vid = "";
	private String aid = "";
	private long time;
	private long timecount;
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

	public static void main(String[] args) throws JsonGenerationException,
			JsonMappingException, IOException {

		ObjectMapper mapper = new ObjectMapper();
		long st = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			VV vv = new VV();
			vv.setTime(137000125688L);
			vv.setAid("123123");
			Video video = vv.getVideo();
			video.setAid("12321");
			video.setMovieid("MID123");
			video.setWid("WID34");
			video.setVtitle("NAME-NAME");

			String json = mapper.writeValueAsString(vv);
			// System.out.println(json);
		}
		long en = System.currentTimeMillis();
		System.out.println(en - st);

		for (int i = 0; i < 1000; i++) {
			String aid = "123123";
			long ts = 137000125688L;
			String title = "NAME-NAME";
			String mid = "MID123";
			String wid = "WID34";
			String json = "{\"vid\":\"\",\"aid\":\""
					+ aid
					+ "\",\"time\":"
					+ ts
					+ ",\"timecount\":0,\"video\":{\"channel\":\"\",\"vid\":\"\",\"aid\":\""
					+ aid + "\",\"vtitle\":\"" + title + "\",\"wid\":\"" + wid
					+ "\",\"pid\":\"\",\"movieid\":\"" + mid + "\"}}";

			// System.out.println(json);
		}
		long en2 = System.currentTimeMillis();
		System.out.println(en2 - en);
	}
}
