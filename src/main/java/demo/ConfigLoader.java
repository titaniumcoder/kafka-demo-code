package demo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {
    public static Properties loadConfig(final String configFile) throws IOException {
        final Properties cfg = new Properties();
        try (InputStream inputStream = ConfigLoader.class.getResourceAsStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}
