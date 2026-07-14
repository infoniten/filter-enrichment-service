package com.example.filterenrichment.domain;

import java.util.Optional;

/**
 * Validates the mandatory fields of an input message per type (§10/§13). A missing field yields a
 * DLQ reason (§29 input DLQ). Absent enum / null message is caught earlier at parse time.
 */
public final class InputMessageValidator {

    private InputMessageValidator() {
    }

    /** @return a reason string if invalid, empty if the message is structurally complete. */
    public static Optional<String> validate(InputMessage msg) {
        if (msg == null) {
            return Optional.of("null message");
        }
        if (msg.messageType() == null) {
            return Optional.of("missing/unknown messageType");
        }
        if (isBlank(msg.sourceEventId())) {
            return Optional.of("missing sourceEventId");
        }
        if (isBlank(msg.objectClass())) {
            return Optional.of("missing objectClass");
        }
        if (isBlank(msg.objectId())) {
            return Optional.of("missing objectId");
        }
        if (isBlank(msg.savedAt())) {
            return Optional.of("missing savedAt");
        }
        return switch (msg.messageType()) {
            case OBJECT -> validateObject(msg);
            case BEFORE_AFTER -> validateBeforeAfter(msg);
        };
    }

    private static Optional<String> validateObject(InputMessage msg) {
        if (msg.globalId() == null) {
            return Optional.of("missing globalId");
        }
        if (msg.revisionId() == null) {
            return Optional.of("missing revisionId");
        }
        if (msg.payload() == null || msg.payload().isNull()) {
            return Optional.of("missing payload");
        }
        return Optional.empty();
    }

    private static Optional<String> validateBeforeAfter(InputMessage msg) {
        if (msg.before() == null) {
            return Optional.of("missing before");
        }
        if (msg.after() == null) {
            return Optional.of("missing after");
        }
        if (msg.before().revisionId() == null) {
            return Optional.of("missing before.revisionId");
        }
        if (msg.before().payload() == null || msg.before().payload().isNull()) {
            return Optional.of("missing before.payload");
        }
        if (msg.after().revisionId() == null) {
            return Optional.of("missing after.revisionId");
        }
        if (msg.after().payload() == null || msg.after().payload().isNull()) {
            return Optional.of("missing after.payload");
        }
        return Optional.empty();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
