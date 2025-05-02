package gr.aueb.client.dtos;

import gr.aueb.dtos.BaseRequest;

/**
 * Client request to buy a product
 */
public class BuyProductClientRequest extends BaseClientRequest {
    private static final long serialVersionUID = 302L;
    private final String storeName;
    private final String productName;
    private final int quantity;

    public BuyProductClientRequest(String storeName, String productName, int quantity) {
        this.storeName = storeName;
        this.productName = productName;
        this.quantity = quantity;
    }

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