package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents the filter criteria sent by the client.
 */
public class FilterCriteria implements Serializable {
    private static final long serialVersionUID = 1L; // Added serialVersionUID

    final double clientLatitude;
    final double clientLongitude;
    final double maxDistance;
    final List<String> foodCategories;
    final int minStars;
    final List<String> priceCategories;

    public FilterCriteria(double clientLatitude, double clientLongitude, double maxDistance,
                          List<String> foodCategories, int minStars, List<String> priceCategories) {
        this.clientLatitude = clientLatitude;
        this.clientLongitude = clientLongitude;
        this.maxDistance = maxDistance;
        this.foodCategories = foodCategories == null ? new ArrayList<>() : new ArrayList<>(foodCategories);
        this.minStars = minStars;
        this.priceCategories = priceCategories == null ? new ArrayList<>() : new ArrayList<>(priceCategories);
    }

    public double getClientLatitude() {
        return clientLatitude;
    }

    public double getClientLongitude() {
        return clientLongitude;
    }

    public double getMaxDistance() {
        return maxDistance;
    }

    public List<String> getFoodCategories() {
        return new ArrayList<>(foodCategories);
    }

    public int getMinStars() {
        return minStars;
    }

    public List<String> getPriceCategories() {
        return new ArrayList<>(priceCategories);
    }
}