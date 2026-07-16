package com.example.filterenrichment;

import com.example.filterenrichment.metamodel.MetamodelCatalog;
import com.example.filterenrichment.metamodel.MetamodelCatalogFactory;
import com.example.filterenrichment.metamodel.dto.MetadataResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared test domain model (small slice of the real one), built the same way as production: parse a
 * DataDictionary {@code /api/search-service/metadata/v3} JSON body into {@link MetadataResponse} and
 * assemble a catalog.
 */
public final class TestFixtures {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String DOMAIN_JSON = """
            {
              "classes": [
                {"name": "TRADE", "sourceValue": "Trade"},
                {"name": "FX_SPOT_FORWARD_TRADE", "sourceValue": "FxSpotForwardTrade"},
                {"name": "CURRENCY", "sourceValue": "Currency"},
                {"name": "COUNTERPARTY", "sourceValue": "Counterparty"},
                {"name": "CASHFLOW", "sourceValue": "Cashflow"}
              ],
              "fields": {
                "TRADE": {"declaredFields": [{"name": "portfolioId", "type": "LONG"}, {"name": "status", "type": "STRING"}, {"name": "contractId", "type": "LONG"}]},
                "FX_SPOT_FORWARD_TRADE": {"declaredFields": [{"name": "baseAmount", "type": "DECIMAL"}]},
                "CURRENCY": {"declaredFields": [{"name": "code", "type": "STRING"}]},
                "COUNTERPARTY": {"declaredFields": [{"name": "code", "type": "STRING"}, {"name": "name", "type": "STRING"}]},
                "CASHFLOW": {"declaredFields": [{"name": "amount", "type": "DECIMAL"}]}
              },
              "hierarchy": {
                "parentsOrSelf": {
                  "TRADE": ["TRADE"],
                  "FX_SPOT_FORWARD_TRADE": ["FX_SPOT_FORWARD_TRADE", "TRADE"],
                  "CURRENCY": ["CURRENCY"],
                  "COUNTERPARTY": ["COUNTERPARTY"],
                  "CASHFLOW": ["CASHFLOW"]
                }
              },
              "relations": {
                "TRADE": [
                  {"name": "counterpartyId", "alias": "counterparty", "type": "GLOBAL_LINK", "targetClass": "COUNTERPARTY"},
                  {"name": "cashflows", "type": "EMBEDDED_SET", "targetClass": "CASHFLOW"}
                ]
              }
            }
            """;

    private TestFixtures() {
    }

    public static MetamodelCatalog catalog() {
        try {
            MetadataResponse domain = MAPPER.readValue(DOMAIN_JSON, MetadataResponse.class);
            return MetamodelCatalogFactory.build(domain);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static JsonNode json(String s) {
        try {
            return MAPPER.readTree(s);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
