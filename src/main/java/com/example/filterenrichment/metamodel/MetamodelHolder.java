package com.example.filterenrichment.metamodel;

import com.example.filterenrichment.config.FilterEnrichmentProperties;
import com.example.filterenrichment.metamodel.dto.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Loads and holds the domain model from DataDictionary (the metamodel source of truth). Metadata is
 * fetched at startup and after config reloads only — never per input message — and kept in memory.
 * Best-effort at startup: if DataDictionary is not yet reachable the pod stays up but reports
 * not-ready until a load succeeds.
 */
@Component
public class MetamodelHolder {

    private static final Logger log = LoggerFactory.getLogger(MetamodelHolder.class);

    private final RestClient restClient;
    private final FilterEnrichmentProperties.Metamodel props;
    private final AtomicReference<MetamodelCatalog> ref = new AtomicReference<>();

    public MetamodelHolder(@Qualifier("metamodelRestClient") RestClient metamodelRestClient,
                           FilterEnrichmentProperties props) {
        this.restClient = metamodelRestClient;
        this.props = props.getMetamodel();
    }

    /** Loads the metadata; throws on failure so callers can decide (startup logs, health reports). */
    public MetamodelCatalog load() {
        MetadataResponse domain;
        try {
            domain = restClient.get().uri(props.getMetadataPath()).retrieve().body(MetadataResponse.class);
        } catch (RestClientException e) {
            throw new IllegalStateException("failed to fetch metamodel from "
                    + props.getBaseUrl() + props.getMetadataPath(), e);
        }
        if (domain == null || domain.classes() == null || domain.classes().isEmpty()) {
            throw new IllegalStateException("empty metamodel response from DataDictionary");
        }
        MetamodelCatalog catalog = MetamodelCatalogFactory.build(domain);
        ref.set(catalog);
        log.info("Loaded domain model from DataDictionary ({}{}): {} classes",
                props.getBaseUrl(), props.getMetadataPath(), catalog.classCount());
        return catalog;
    }

    public boolean isLoaded() {
        return ref.get() != null;
    }

    /** Current catalog, or throws if not yet loaded. */
    public MetamodelCatalog get() {
        MetamodelCatalog catalog = ref.get();
        if (catalog == null) {
            throw new IllegalStateException("domain model not loaded");
        }
        return catalog;
    }
}
