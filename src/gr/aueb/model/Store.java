package gr.aueb.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a Store managed by this Worker.
 */
public class Store implements Serializable {
    private static final long serialVersionUID = 3L;

    private String storeName;
    private double latitude;
    private double longitude;
    private String foodCategory;
    private int stars;
    private int noOfVotes;
    private String storeLogoPath;
    private Map<String, Product> products = new HashMap<>();
    private String priceCategory;

    public synchronized void calculateAndSetPriceCategory() {
        if (getProducts().isEmpty()) {
            this.setPriceCategory("$");
            return;
        }
        double averagePrice = getProducts().values().stream().mapToDouble(p -> p.price).average().orElse(0.0);
        if (averagePrice <= 5.0) {
            this.setPriceCategory("$");
        } else if (averagePrice <= 15.0) {
            this.setPriceCategory("$$");
        } else {
            this.setPriceCategory("$$$");
        }
    }

    public synchronized void addProduct(Product product) {
        getProducts().put(product.getProductName(), product);
        calculateAndSetPriceCategory();
    }

    public synchronized Product removeProduct(String productName) {
        Product removedProduct = getProducts().remove(productName);
        if (removedProduct != null) {
            calculateAndSetPriceCategory();
        }
        return removedProduct;
    }

    public synchronized Product getProduct(String productName) {
        return getProducts().get(productName);
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getFoodCategory() {
        return foodCategory;
    }

    public void setFoodCategory(String foodCategory) {
        this.foodCategory = foodCategory;
    }

    public int getStars() {
        return stars;
    }

    public void setStars(int stars) {
        this.stars = stars;
    }

    public int getNoOfVotes() {
        return noOfVotes;
    }

    public void setNoOfVotes(int noOfVotes) {
        this.noOfVotes = noOfVotes;
    }

    public String getStoreLogoPath() {
        return storeLogoPath;
    }

    public void setStoreLogoPath(String storeLogoPath) {
        this.storeLogoPath = storeLogoPath;
    }

    public Map<String, Product> getProducts() {
        return products;
    }

    public void setProducts(Map<String, Product> products) {
        this.products = products;
    }

    public String getPriceCategory() {
        return priceCategory;
    }

    public void setPriceCategory(String priceCategory) {
        this.priceCategory = priceCategory;
    }
}