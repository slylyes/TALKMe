package talkme.http;

import com.fasterxml.jackson.databind.DeserializationFeature;
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
            throw new RuntimeException("Error from remote node: HTTP " + response.statusCode() +
                                          " - " + errorMessage.getMessage());
        }

        // Handle empty successful responses
        if (response.body() == null || response.body().isEmpty()) {
            throw new RuntimeException("Empty response body from remote node: " + url);
        }

        // Parse the response body with better error handling
        try {
            return objectMapper.readValue(response.body(), responseType);
        } catch (Exception e) {
            System.err.println("Failed to parse response: " + e.getMessage());
            System.err.println("Response body: " + response.body());
            
            // For better diagnostics, try to see if the response is at least valid JSON
            try {
                Object jsonObject = objectMapper.readTree(response.body());
                System.err.println("Response is valid JSON but cannot be converted to " + responseType.getName());
            } catch (Exception jsonEx) {
                System.err.println("Response is not valid JSON: " + jsonEx.getMessage());
            }
            
            throw new RuntimeException("Failed to parse response from " + url + 
                                      ": " + e.getMessage() + 
                                      "\nResponse body: " + response.body(), e);
        }
    }

    public static <T> T get(ConfigurationManager.NodeConfig node, String path, Class<T> responseType) throws IOException, InterruptedException {
        String url = node.getUrl() + path;
        System.out.println("GET request to: " + url);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        System.out.println("Response status code: " + response.statusCode());
        System.out.println("Response body: " + (response.body() != null ? response.body() : "null"));
        
        if (response.statusCode() >= 400) {
            if (response.body() == null || response.body().isEmpty()) {
                throw new RuntimeException("Error from remote node: HTTP " + response.statusCode() + 
                                          " - Empty response body from " + url);
            }
            
            try {
                StatusMessage errorMessage = objectMapper.readValue(response.body(), StatusMessage.class);
                throw new RuntimeException("Error from remote node: HTTP " + response.statusCode() + 
                                          " - " + errorMessage);
            } catch (Exception e) {
                throw new RuntimeException("Error from remote node: HTTP " + response.statusCode() + 
                                         " - Raw response: " + response.body());
            }
        }
        
        if (response.body() == null || response.body().isEmpty()) {
            throw new RuntimeException("Empty response body from remote node: " + url);
        }
        
        return objectMapper.readValue(response.body(), responseType);
    }
    
    public static <T> T uploadFile(ConfigurationManager.NodeConfig node, String path, File file, String tableName, int limit, Class<T> responseType) throws IOException, InterruptedException {
        String url = node.getUrl() + path + "?tableName=" + tableName + "&limit=" + limit;
        System.out.println("File upload to: " + url);
        System.out.println("File size: " + file.length() + " bytes");
        
        byte[] fileContent = Files.readAllBytes(file.toPath());
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(fileContent))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        System.out.println("Response status code: " + response.statusCode());
        System.out.println("Response body: " + (response.body() != null ? response.body() : "null"));
        
        if (response.statusCode() >= 400) {
            if (response.body() == null || response.body().isEmpty()) {
                throw new RuntimeException("Error from remote node: HTTP " + response.statusCode() + 
                                          " - Empty response body from " + url);
            }
            
            try {
                StatusMessage errorMessage = objectMapper.readValue(response.body(), StatusMessage.class);
                throw new RuntimeException("Error from remote node: HTTP " + response.statusCode() + 
                                          " - " + errorMessage);
            } catch (Exception e) {
                throw new RuntimeException("Error from remote node: HTTP " + response.statusCode() + 
                                         " - Raw response: " + response.body());
            }
        }

        if (response.body() == null || response.body().isEmpty()) {
            throw new RuntimeException("Empty response body from remote node: " + url);
        }
        
        return objectMapper.readValue(response.body(), responseType);
    }
}

