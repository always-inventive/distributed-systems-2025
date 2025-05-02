package gr.aueb.manager.dtos;

import java.io.Serializable;

public class UpdateProductRequest extends BaseManagerRequest { // Reverted name
    private static final long serialVersionUID = 102L;

    public enum Action {ADD, REMOVE, UPDATE_STOCK}

    private final Action action;
    private final String storeName;
    private final String productName;
    // Include details needed for ADD/UPDATE
    private final String productType; // For ADD
    private final int amount;         // For ADD/UPDATE_STOCK
    private final double price;       // For ADD

    // Constructor for ADD
    public UpdateProductRequest(Action action, String storeName, String productName, String productType, int amount, double price) {
        if (action != Action.ADD) throw new IllegalArgumentException("Incorrect constructor for action: " + action);
        this.action = action;
        this.storeName = storeName;
        this.productName = productName;
        this.productType = productType;
        this.amount = amount;
        this.price = price;
    }

    // Constructor for REMOVE
    public UpdateProductRequest(Action action, String storeName, String productName) {
        if (action != Action.REMOVE) throw new IllegalArgumentException("Incorrect constructor for action: " + action);
        this.action = action;
        this.storeName = storeName;
        this.productName = productName;
        this.productType = null;
        this.amount = 0;
        this.price = 0.0;
    }

    // Constructor for UPDATE_STOCK
    public UpdateProductRequest(Action action, String storeName, String productName, int newAmount) {
        if (action != Action.UPDATE_STOCK)
            throw new IllegalArgumentException("Incorrect constructor for action: " + action);
        this.action = action;
        this.storeName = storeName;
        this.productName = productName;
        this.amount = newAmount;
        this.productType = null;
        this.price = 0.0;
    }

    // Getters...
    public Action getAction() {
        return action;
    }

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
