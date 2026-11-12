package io.openkache.core.replication.ionia;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;

/** The key-based operations supported by IONIA and its workloads. */
public final class IoniaCommands {
    private IoniaCommands() {}

    public static boolean supports(Command command) {
        return command instanceof Command.Get || isWrite(command);
    }

    static boolean isWrite(Command command) {
        return command instanceof Command.Set || command instanceof Command.Delete;
    }

    public static Bytes key(Command command) {
        return switch (command) {
            case Command.Get get -> get.key();
            case Command.Set set -> set.key();
            case Command.Delete delete -> delete.key();
            default -> throw new IllegalArgumentException("Unsupported IONIA command: " + command);
        };
    }
}
