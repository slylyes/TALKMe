package talkme.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StatusMessage {
    @JsonProperty
    private String message;

    public StatusMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}