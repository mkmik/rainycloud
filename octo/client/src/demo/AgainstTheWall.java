package demo;

import org.json.simple.JSONObject;
import com.urbanairship.octobot.Settings;

public class AgainstTheWall {

	public static void run(JSONObject task) {
		String what = Settings.get("BangYourHead", "what");

		System.out.println("against the "+what+":" + task);
	}
}