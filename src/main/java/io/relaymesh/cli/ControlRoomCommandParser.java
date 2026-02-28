package io.relaymesh.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class ControlRoomCommandParser {
    private ControlRoomCommandParser() {
    }

    static List<String> parseTokens(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return out;
        }
        for (String token : raw.trim().split("\\s+")) {
            if (token != null && !token.isBlank()) {
                out.add(token.trim());
            }
        }
        return out;
    }

    static String joinTail(List<String> tokens, int startIndex) {
        if (tokens == null || tokens.isEmpty() || startIndex >= tokens.size()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = startIndex; i < tokens.size(); i++) {
            if (i > startIndex) {
                sb.append(' ');
            }
            sb.append(tokens.get(i));
        }
        return sb.toString();
    }

    static boolean isWriteCommand(String op) {
        if (op == null || op.isBlank()) {
            return false;
        }
        String value = op.trim().toLowerCase(Locale.ROOT);
        return "cancel".equals(value)
                || "replay".equals(value)
                || "replay-batch".equals(value)
                || "replay_batch".equals(value);
    }
}

