package gr.aueb.manager.dtos;

import gr.aueb.dtos.BaseRequest;

import java.io.Serializable;

/**
 * Manager request to remove a product
 */
public class RemoveProductManagerRequest extends BaseManagerRequest {
    private static final long serialVersionUID = 312L;
    private final String storeName;
    private final String productName;

    public RemoveProductManagerRequest(String sn, String pn) {
        this.storeName = sn;
        this.productName = pn;
    }

    public String getStoreName() {
        return storeName;
    }

    public String getProductName() {
        return productName;
    }
}