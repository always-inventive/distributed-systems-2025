package gr.aueb.client.dtos;

import gr.aueb.dtos.BaseResponse;

import java.io.Serializable;

/**
 * Response from Worker to Master after a purchase attempt.
 */
public class PurchaseResponse extends BaseResponse {
    private static final long serialVersionUID = 201L;

    public enum Status {
        OK, FAIL_STOCK, FAIL_PRODUCT_NOT_FOUND, FAIL_STORE_NOT_FOUND, FAIL_INVALID_QTY, FAIL_WORKER_ERROR
    }

    private final Status status;
    private final String message; // Optional additional info

    public PurchaseResponse(Status status, String message) {
        this.status = status;
        this.message = message;
    }

    public PurchaseResponse(Status status) {
        this(status, null);
    }

    // Getters
    public Status getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    // Convenience method to convert back to string codes used by client app
    public String toStatusCodeString() {
        switch (status) {
            case OK:
                return "PURCHASE_OK";
            case FAIL_STOCK:
                return "PURCHASE_FAIL_STOCK";
            case FAIL_PRODUCT_NOT_FOUND:
                return "PURCHASE_FAIL_PRODUCT_NOT_FOUND";
            case FAIL_STORE_NOT_FOUND:
                return "PURCHASE_FAIL_STORE_NOT_FOUND";
            case FAIL_INVALID_QTY:
                return "PURCHASE_FAIL_INVALID_QTY";
            case FAIL_WORKER_ERROR:
            default:
                return "PURCHASE_FAIL_WORKER_ERROR"; // Generic error
        }
    }
}
