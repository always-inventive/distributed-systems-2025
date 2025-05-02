package gr.aueb.model;

import java.io.Serializable;

/**
 * Represents a Product within a Store.
 */
public class Product implements Serializable {
    private static final long serialVersionUID = 2L; // Added serialVersionUID

    public String productName;
    public String productType;
    public int availableAmount;
    public double price;

    public Product(String productName, String productType, int availableAmount, double price) {
        this.productName = productName;
        this.productType = productType;
        this.availableAmount = availableAmount;
        this.price = price;
    }

    public String getProductName() {
        return productName;
    }

    public String getProductType() {
        return productType;
    }

    public int getAvailableAmount() {
        return availableAmount;
    }

    public double getPrice() {
        return price;
    }

    // Synchronized methods for stock management
    public synchronized boolean decreaseStock(int quantity) {
        if (this.availableAmount >= quantity) {
            this.availableAmount -= quantity;
            return true;
        }
        return false;
    }

    public synchronized void increaseStock(int quantity) {
        this.availableAmount += quantity;
    }

    public synchronized void setStock(int newAmount) {
        if (newAmount >= 0) {
            this.availableAmount = newAmount;
        }
    }
}