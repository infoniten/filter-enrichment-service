package com.example.filterenrichment.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InputMessageParserTest {

    private final InputMessageParser parser = new InputMessageParser(new ObjectMapper());

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void parsesBareObjectAsObject() {
        InputMessage msg = parser.parse(bytes("""
                {"objectType":"FxSpotForwardTrade","globalId":110038431,"id":201027932,"revision":1667,
                 "revisionEventId":"110640124","savedAt":"2026-07-14T17:04:03.404+03:00",
                 "portfolioId":6052,"status":"LIVE"}
                """));

        assertThat(msg.messageType()).isEqualTo(MessageType.OBJECT);
        assertThat(msg.objectClass()).isEqualTo("FxSpotForwardTrade");
        assertThat(msg.globalId()).isEqualTo(110038431L);
        assertThat(msg.objectId()).isEqualTo("110038431");     // objectId <- globalId
        assertThat(msg.revisionId()).isEqualTo(201027932L);    // revisionId <- id
        assertThat(msg.payload().get("portfolioId").asInt()).isEqualTo(6052);
    }

    @Test
    void parsesBeforeAfter() {
        InputMessage msg = parser.parse(bytes("""
                {"before":{"objectType":"FxSpotForwardTrade","globalId":110038431,"id":201027900,
                           "revisionEventId":"110640123","savedAt":"2026-07-14T17:03:59.101+03:00","portfolioId":6052},
                 "after":{"objectType":"FxSpotForwardTrade","globalId":110038431,"id":201027932,
                          "revisionEventId":"110640124","savedAt":"2026-07-14T17:04:03.404+03:00","portfolioId":6052}}
                """));

        assertThat(msg.messageType()).isEqualTo(MessageType.BEFORE_AFTER);
        assertThat(msg.before().revisionId()).isEqualTo(201027900L); // distinct version ids
        assertThat(msg.after().revisionId()).isEqualTo(201027932L);
        assertThat(msg.objectId()).isEqualTo("110038431");
    }

    @Test
    void objectClassFieldTakesPrecedenceOverObjectType() {
        InputMessage msg = parser.parse(bytes(
                "{\"objectClass\":\"FxNdfTrade\",\"objectType\":\"FxSpotForwardTrade\",\"globalId\":1,\"id\":2}"));
        assertThat(msg.objectClass()).isEqualTo("FxNdfTrade");
    }

    @Test
    void rejectsMalformedAndIncomplete() {
        assertThatThrownBy(() -> parser.parse(bytes("not json")))
                .isInstanceOf(InputParseException.class);
        assertThatThrownBy(() -> parser.parse(bytes("{\"objectType\":\"X\",\"id\":2}"))) // no globalId
                .isInstanceOf(InputParseException.class);
        assertThatThrownBy(() -> parser.parse(bytes("{\"before\":{\"objectType\":\"X\",\"globalId\":1,\"id\":2}}")))
                .isInstanceOf(InputParseException.class); // before without after
    }
}
