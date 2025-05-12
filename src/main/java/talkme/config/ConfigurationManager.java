package talkme.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConfigurationManager {
    private static final Properties properties = new Properties();
    private static ConfigurationManager instance;
    private final List<NodeConfig> nodes = new ArrayList<>();
    private int currentNodeId;
    private boolean distributedEnabled;

    private ConfigurationManager() {
        loadProperties();
    }

    public static ConfigurationManager getInstance() {
        if (instance == null) {
            instance = new ConfigurationManager();
        }
        return instance;
    }

    private void loadProperties() {
        try (InputStream input = ConfigurationManager.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.err.println("Unable to find application.properties");
                return;
            }
            properties.load(input);
            
            distributedEnabled = Boolean.parseBoolean(properties.getProperty("distributed.enabled", "false"));
            currentNodeId = Integer.parseInt(properties.getProperty("current.node.id", "1"));
            
            int nodeCount = Integer.parseInt(properties.getProperty("distributed.node.count", "1"));
            for (int i = 1; i <= nodeCount; i++) {
                String ip = properties.getProperty("distributed.node." + i + ".ip");
                int port = Integer.parseInt(properties.getProperty("distributed.node." + i + ".port", "8080"));
                nodes.add(new NodeConfig(i, ip, port));
            }
        } catch (IOException e) {
            System.err.println("Error loading application.properties: " + e.getMessage());
        }
    }

    public List<NodeConfig> getNodes() {
        return nodes;
    }

    public NodeConfig getNodeById(int id) {
        return nodes.stream()
                .filter(node -> node.getId() == id)
                .findFirst()
                .orElse(null);
    }

    public boolean isDistributedEnabled() {
        return distributedEnabled;
    }

    public int getCurrentNodeId() {
        return currentNodeId;
    }

    public static class NodeConfig {
        private final int id;
        private final String ip;
        private final int port;

        public NodeConfig(int id, String ip, int port) {
            this.id = id;
            this.ip = ip;
            this.port = port;
        }

        public int getId() {
            return id;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public String getUrl() {
            return "http://" + ip + ":" + port;
        }
    }
}
