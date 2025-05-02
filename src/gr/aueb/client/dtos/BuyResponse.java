package gr.aueb.client.dtos;

import gr.aueb.dtos.BaseResponse;

public class BuyResponse extends BaseResponse { // Reverted name
    private static final long serialVersionUID = 402L;
    private final String statusCode; // e.g., "PURCHASE_OK", "PURCHASE_FAIL_STOCK"

    public BuyResponse(String statusCode) {
        this.statusCode = statusCode;
    }

    public String getStatusCode() {
        return statusCode;
    }
}
