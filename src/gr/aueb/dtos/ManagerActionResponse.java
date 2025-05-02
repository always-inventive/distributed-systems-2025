package gr.aueb.dtos;

import java.io.Serializable;

public class ManagerActionResponse implements Serializable {
    private static final long serialVersionUID = 410L;
    private final boolean success;
    private final String message;

    public ManagerActionResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }
}
