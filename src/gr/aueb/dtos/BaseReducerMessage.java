package gr.aueb.dtos;

import java.io.Serializable;

/**
 * Base class for messages sent to the Reducer.
 */
public abstract class BaseReducerMessage implements Serializable {
    private static final long serialVersionUID = 50L;
    private final String jobId;

    protected BaseReducerMessage(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }
}