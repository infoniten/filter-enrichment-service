package com.example.filterenrichment.filter;

/**
 * Three-valued (Kleene) logic used for pre-matching against the flat input payload: a
 * comparison whose field is present in the flat payload evaluates to {@link #TRUE}/{@link #FALSE};
 * one whose field is only available after enrichment is {@link #UNKNOWN}. A subscription is excluded
 * as a non-candidate only when its pre-match is definitively {@code FALSE}.
 */
public enum Tri {
    TRUE,
    FALSE,
    UNKNOWN;

    public static Tri of(boolean b) {
        return b ? TRUE : FALSE;
    }

    /** AND: FALSE if any operand is FALSE; else UNKNOWN if any is UNKNOWN; else TRUE. */
    public Tri and(Tri other) {
        if (this == FALSE || other == FALSE) {
            return FALSE;
        }
        if (this == UNKNOWN || other == UNKNOWN) {
            return UNKNOWN;
        }
        return TRUE;
    }

    /** OR: TRUE if any operand is TRUE; else UNKNOWN if any is UNKNOWN; else FALSE. */
    public Tri or(Tri other) {
        if (this == TRUE || other == TRUE) {
            return TRUE;
        }
        if (this == UNKNOWN || other == UNKNOWN) {
            return UNKNOWN;
        }
        return FALSE;
    }

    /** Whether this state definitively excludes a candidate (only a hard FALSE does). */
    public boolean isDefinitelyFalse() {
        return this == FALSE;
    }
}
