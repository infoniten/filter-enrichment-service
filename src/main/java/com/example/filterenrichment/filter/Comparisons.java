package com.example.filterenrichment.filter;

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.List;

/**
 * Type-aware comparison of a JSON scalar value against RSQL string arguments.
 */
final class Comparisons {

    private Comparisons() {
    }

    static boolean matches(JsonNode value, String operator, List<String> args) {
        String arg = args.isEmpty() ? null : args.get(0);
        switch (operator) {
            case "==":
                return valueEquals(value, arg);
            case "!=":
                return !valueEquals(value, arg);
            case "=in=":
                return args.stream().anyMatch(a -> valueEquals(value, a));
            case "=out=":
                return args.stream().noneMatch(a -> valueEquals(value, a));
            case "=gt=": {
                Integer c = compare(value, arg);
                return c != null && c > 0;
            }
            case "=ge=": {
                Integer c = compare(value, arg);
                return c != null && c >= 0;
            }
            case "=lt=": {
                Integer c = compare(value, arg);
                return c != null && c < 0;
            }
            case "=le=": {
                Integer c = compare(value, arg);
                return c != null && c <= 0;
            }
            default:
                return false;
        }
    }

    private static boolean isAbsent(JsonNode v) {
        return v == null || v.isNull() || v.isMissingNode();
    }

    private static boolean valueEquals(JsonNode v, String arg) {
        if (isAbsent(v)) {
            return "null".equalsIgnoreCase(arg);
        }
        if (v.isNumber()) {
            BigDecimal a = parseDecimal(arg);
            return a != null && v.decimalValue().compareTo(a) == 0;
        }
        if (v.isBoolean()) {
            return v.asBoolean() == Boolean.parseBoolean(arg);
        }
        return v.asText().equals(arg);
    }

    /** Returns sign of (value - arg), or null if not comparable / value absent. */
    private static Integer compare(JsonNode v, String arg) {
        if (isAbsent(v) || arg == null) {
            return null;
        }
        if (v.isNumber()) {
            BigDecimal a = parseDecimal(arg);
            return a == null ? null : Integer.signum(v.decimalValue().compareTo(a));
        }
        return Integer.signum(v.asText().compareTo(arg));
    }

    private static BigDecimal parseDecimal(String s) {
        if (s == null) {
            return null;
        }
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
