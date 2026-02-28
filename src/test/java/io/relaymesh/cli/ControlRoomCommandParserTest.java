package io.relaymesh.cli;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlRoomCommandParserTest {
    @Test
    void parseTokensShouldSplitByWhitespace() {
        List<String> tokens = ControlRoomCommandParser.parseTokens("  cancel   default   task-1   hard  reason here  ");
        assertEquals(List.of("cancel", "default", "task-1", "hard", "reason", "here"), tokens);
    }

    @Test
    void joinTailShouldReturnRemainder() {
        List<String> tokens = List.of("cancel", "default", "task-1", "soft", "manual", "ops");
        assertEquals("manual ops", ControlRoomCommandParser.joinTail(tokens, 4));
        assertEquals("", ControlRoomCommandParser.joinTail(tokens, 9));
    }

    @Test
    void isWriteCommandShouldRecognizeMutations() {
        assertTrue(ControlRoomCommandParser.isWriteCommand("cancel"));
        assertTrue(ControlRoomCommandParser.isWriteCommand("replay"));
        assertTrue(ControlRoomCommandParser.isWriteCommand("replay-batch"));
        assertTrue(ControlRoomCommandParser.isWriteCommand("replay_batch"));
        assertFalse(ControlRoomCommandParser.isWriteCommand("stats"));
        assertFalse(ControlRoomCommandParser.isWriteCommand("help"));
    }
}

