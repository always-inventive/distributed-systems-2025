package gr.aueb.manager.dtos;

import gr.aueb.dtos.BaseRequest;

/**
 * Manager request to add a product
 */
public class AddProductManagerRequest extends BaseManagerRequest {
    private static final long serialVersionUID = 311L;
    private final String storeName;
    private final String productName;
    private final String productType;
    private final int amount;
    private final double price;

    public AddProductManagerRequest(String sn, String pn, String pt, int a, double p) {
        this.storeName = sn;
        this.productName = pn;
        this.productType = pt;
        this.amount = a;
        this.price = p;
    }

    // Getters...
    public String getStoreName() {
        return storeName;
    }

    public String getProductName() {
        return productName;
    }

    public String getProductType() {
        return productType;
    }

    public int getAmount() {
        return amount;
    }

    public double getPrice() {
        return price;
    }
}