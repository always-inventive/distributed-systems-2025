package gr.aueb.manager.dtos;

import gr.aueb.dtos.BaseRequest;

import java.io.Serializable;

/**
 * Manager request to update stock
 */
public class UpdateStockManagerRequest extends BaseManagerRequest {
    private static final long serialVersionUID = 313L;
    private final String storeName;
    private final String productName;
    private final int newAmount;

    public UpdateStockManagerRequest(String sn, String pn, int amount) {
        this.storeName = sn;
        this.productName = pn;
        this.newAmount = amount;
    }

    public String getStoreName() {
        return storeName;
    }

    public String getProductName() {
        return productName;
    }

    public int getNewAmount() {
        return newAmount;
    }
}