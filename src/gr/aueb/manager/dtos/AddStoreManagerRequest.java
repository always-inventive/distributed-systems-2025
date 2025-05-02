package gr.aueb.manager.dtos;

import gr.aueb.dtos.BaseRequest;

/**
 * Manager request to add/update a store (sending JSON as string for now)
 */
public class AddStoreManagerRequest extends BaseManagerRequest {
    private static final long serialVersionUID = 310L;
    private final String storeJson; // Keep sending JSON from gr.aueb.manager for simplicity

    public AddStoreManagerRequest(String storeJson) {
        this.storeJson = storeJson;
    }

    public String getStoreJson() {
        return storeJson;
    }
}