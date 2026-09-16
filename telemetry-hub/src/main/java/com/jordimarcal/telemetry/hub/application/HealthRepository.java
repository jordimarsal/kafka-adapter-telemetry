package com.jordimarcal.telemetry.hub.application;

import com.jordimarcal.telemetry.contracts.AdapterId;
import com.jordimarcal.telemetry.hub.domain.AdapterHealth;

/**
 * Persistence of the per-adapter health aggregate. {@link #find} never returns
 * null: an unseen adapter is simply in its initial state.
 */
public interface HealthRepository {

    AdapterHealth find(AdapterId adapterId);

    void save(AdapterHealth health);

    /**
     * Demo-reset only: forgets every adapter so the wall starts empty again.
     * Never called from the ingest path.
     */
    void clear();
}
