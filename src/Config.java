import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

public class Config {
    public int numPublications;
    public int numSubscriptions;
    public int numThreads;
    public int equalityRatio;
    public Map<String, Integer> fieldFrequencies = new LinkedHashMap<>();

    public Config(String configPath) {
        String content = null;
        try {
            content = new String(Files.readAllBytes(Paths.get(configPath)));
            JSONObject json = new JSONObject(content);

            this.numPublications = json.optInt("numPublications", 10);
            this.numSubscriptions = json.optInt("numSubscriptions", 10);
            this.numThreads = json.optInt("numThreads", 4);
            this.equalityRatio = json.optInt("equalityRatio", 70);

            JSONObject freqJson = json.optJSONObject("fieldFrequencies", null);
            if (freqJson != null) {
                for (String key : freqJson.keySet()) {
                    fieldFrequencies.put(key, freqJson.getInt(key));
                }
            } else {
                fieldFrequencies.put("city", 90);
                fieldFrequencies.put("temp", 70);
                fieldFrequencies.put("wind", 30);
                fieldFrequencies.put("date", 30);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
