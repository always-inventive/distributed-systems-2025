package gr.aueb.manager.dtos;

import gr.aueb.dtos.BaseRequest;
import gr.aueb.model.Store;

/**
 * Request to store/update data for a whole store.
 */
public class StoreDataRequest extends BaseRequest {
    private static final long serialVersionUID = 101L;
    private final Store store; // Send the whole Store object

    public StoreDataRequest(Store store) {
        this.store = store;
    }

    public Store getStore() {
        return store;
    }
}
