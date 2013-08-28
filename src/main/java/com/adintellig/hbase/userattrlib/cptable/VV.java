package com.adintellig.hbase.userattrlib.cptable;

import java.io.IOException;
import java.lang.Character.UnicodeBlock;

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
		for (int i = 0; i < 10; i++) {
			VV vv = new VV();
			vv.setTime(137000125688L);
			vv.setAid("123123");
			Video video = vv.getVideo();
			video.setAid("12321");
			video.setVtitle("中国 加拿大");
			video.setMovieid("MID123");
			video.setWid("WID34");
//			video.setVtitle("NAME-NAME");

			String json = mapper.writeValueAsString(vv);
			json = utf8ToUnicode(json);
			System.out.println(json);
			
			System.out.println(unicodeToUtf8("\u57A0"));
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
	
	/**
	 * utf8 -> unicode
	 * 
	 * @param inStr
	 * @return String
	 */
	public static String utf8ToUnicode(String inStr) {
		char[] myBuffer = inStr.toCharArray();

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < inStr.length(); i++) {
			UnicodeBlock ub = UnicodeBlock.of(myBuffer[i]);
			if (ub == UnicodeBlock.BASIC_LATIN) {
				sb.append(myBuffer[i]);
			} else if (ub == UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
				int j = (int) myBuffer[i] - 65248;
				sb.append((char) j);
			} else {
				short s = (short) myBuffer[i];
				String hexS = Integer.toHexString(s);
				String unicode = "\\u" + hexS;
				sb.append(unicode.toLowerCase());
			}
		}
		return sb.toString();
	}
	
	/**
	 * unicode -> utf8
	 * @param theString
	 * @return String
	 */
	public static String unicodeToUtf8(String theString) {
		char aChar;
		int len = theString.length();
		StringBuffer outBuffer = new StringBuffer(len);
		for (int x = 0; x < len;) {
			aChar = theString.charAt(x++);
			if (aChar == '\\') {
				aChar = theString.charAt(x++);
				if (aChar == 'u') {
					// Read the xxxx
					int value = 0;
					for (int i = 0; i < 4; i++) {
						aChar = theString.charAt(x++);
						switch (aChar) {
						case '0':
						case '1':
						case '2':
						case '3':
						case '4':
						case '5':
						case '6':
						case '7':
						case '8':
						case '9':
							value = (value << 4) + aChar - '0';
							break;
						case 'a':
						case 'b':
						case 'c':
						case 'd':
						case 'e':
						case 'f':
							value = (value << 4) + 10 + aChar - 'a';
							break;
						case 'A':
						case 'B':
						case 'C':
						case 'D':
						case 'E':
						case 'F':
							value = (value << 4) + 10 + aChar - 'A';
							break;
						default:
							throw new IllegalArgumentException(
									"Malformed   \\uxxxx   encoding.");
						}
					}
					outBuffer.append((char) value);
				} else {
					if (aChar == 't')
						aChar = '\t';
					else if (aChar == 'r')
						aChar = '\r';
					else if (aChar == 'n')
						aChar = '\n';
					else if (aChar == 'f')
						aChar = '\f';
					outBuffer.append(aChar);
				}
			} else
				outBuffer.append(aChar);
		}
		return outBuffer.toString();
	}
}
