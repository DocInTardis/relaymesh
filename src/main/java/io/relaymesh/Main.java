package io.relaymesh;

import io.relaymesh.cli.RelayMeshCommand;
import picocli.CommandLine;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        int code = new CommandLine(new RelayMeshCommand()).execute(args);
        System.exit(code);
    }
}

