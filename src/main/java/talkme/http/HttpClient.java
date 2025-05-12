package talkme.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import talkme.api.StatusMessage;
import talkme.config.ConfigurationManager;

import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.time.Duration;

public class HttpClient {
    private static final java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
            .version(java.net.http.HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T post(ConfigurationManager.NodeConfig node, String path, Object requestBody, Class<T> responseType) throws IOException, InterruptedException {
        String url = node.getUrl() + path;
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestBody)))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() >= 400) {
            StatusMessage errorMessage = objectMapper.readValue(response.body(), StatusMessage.class);
            throw new RuntimeException("Error from remote node: " + errorMessage);
        }
        
        return objectMapper.readValue(response.body(), responseType);
    }

    public static <T> T get(ConfigurationManager.NodeConfig node, String path, Class<T> responseType) throws IOException, InterruptedException {
        String url = node.getUrl() + path;
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() >= 400) {
            StatusMessage errorMessage = objectMapper.readValue(response.body(), StatusMessage.class);
            throw new RuntimeException("Error from remote node: " + errorMessage);
        }
        
        return objectMapper.readValue(response.body(), responseType);
    }
    
    public static <T> T uploadFile(ConfigurationManager.NodeConfig node, String path, File file, String tableName, int limit, Class<T> responseType) throws IOException, InterruptedException {
        String url = node.getUrl() + path + "?tableName=" + tableName + "&limit=" + limit;
        
        byte[] fileContent = Files.readAllBytes(file.toPath());
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(fileContent))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() >= 400) {
            StatusMessage errorMessage = objectMapper.readValue(response.body(), StatusMessage.class);
            throw new RuntimeException("Error from remote node: " + errorMessage);
        }
        
        return objectMapper.readValue(response.body(), responseType);
    }
}
