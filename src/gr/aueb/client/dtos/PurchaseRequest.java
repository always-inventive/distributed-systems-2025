package gr.aueb.client.dtos;

import gr.aueb.dtos.BaseRequest;

/**
 * Request for a worker to process a purchase.
 */
public class PurchaseRequest extends BaseRequest {
    private static final long serialVersionUID = 104L;
    private final String storeName;
    private final String productName;
    private final int quantity;

    public PurchaseRequest(String storeName, String productName, int quantity) {
        this.storeName = storeName;
        this.productName = productName;
        this.quantity = quantity;
    }

    // Getters
    public String getStoreName() {
        return storeName;
    }

    public String getProductName() {
        return productName;
    }

    public int getQuantity() {
        return quantity;
    }
}
