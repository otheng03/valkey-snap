package io.otheng.valkeysnap.model.command;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a Redis command with its arguments.
 * Commands can be serialized to RESP format for sending to a Redis server.
 */
public record Command(String name, List<byte[]> args) {

    public Command {
        name = name != null ? name.toUpperCase() : "";
        args = args != null ? copyArgs(args) : List.of();
    }

    private static List<byte[]> copyArgs(List<byte[]> original) {
        List<byte[]> copy = new ArrayList<>(original.size());
        for (byte[] arg : original) {
            copy.add(arg != null ? arg.clone() : new byte[0]);
        }
        return List.copyOf(copy);
    }

    /**
     * Creates a command from string arguments.
     *
     * @param name the command name
     * @param args the arguments as strings
     * @return the command
     */
    public static Command of(String name, String... args) {
        List<byte[]> byteArgs = new ArrayList<>(args.length);
        for (String arg : args) {
            byteArgs.add(arg.getBytes(StandardCharsets.UTF_8));
        }
        return new Command(name, byteArgs);
    }

    /**
     * Creates a command from byte array arguments.
     *
     * @param name the command name
     * @param args the arguments as byte arrays
     * @return the command
     */
    public static Command ofBytes(String name, byte[]... args) {
        return new Command(name, Arrays.asList(args));
    }

    /**
     * Creates a command from a mixed list of String and byte[] arguments.
     *
     * @param name the command name
     * @param args arguments (String or byte[])
     * @return the command
     */
    public static Command ofMixed(String name, Object... args) {
        List<byte[]> byteArgs = new ArrayList<>(args.length);
        for (Object arg : args) {
            if (arg instanceof String s) {
                byteArgs.add(s.getBytes(StandardCharsets.UTF_8));
            } else if (arg instanceof byte[] b) {
                byteArgs.add(b);
            } else if (arg instanceof Number n) {
                byteArgs.add(n.toString().getBytes(StandardCharsets.UTF_8));
            } else {
                throw new IllegalArgumentException("Unsupported argument type: " + arg.getClass());
            }
        }
        return new Command(name, byteArgs);
    }

    public List<byte[]> args() {
        return copyArgs(args);
    }

    /**
     * Returns the arguments as strings.
     */
    public List<String> argsAsStrings() {
        return args.stream()
            .map(a -> new String(a, StandardCharsets.UTF_8))
            .toList();
    }

    /**
     * Serializes this command to RESP format.
     *
     * @return the RESP-encoded command as bytes
     */
    public byte[] toResp() {
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(args.size() + 1).append("\r\n");

        // Command name
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        sb.append('$').append(nameBytes.length).append("\r\n");
        sb.append(name).append("\r\n");

        // Build the result
        byte[] prefix = sb.toString().getBytes(StandardCharsets.UTF_8);

        // Calculate total size
        int totalSize = prefix.length;
        for (byte[] arg : args) {
            // $<len>\r\n<data>\r\n
            totalSize += 1 + String.valueOf(arg.length).length() + 2 + arg.length + 2;
        }

        byte[] result = new byte[totalSize];
        int pos = 0;
        System.arraycopy(prefix, 0, result, pos, prefix.length);
        pos += prefix.length;

        for (byte[] arg : args) {
            byte[] header = ("$" + arg.length + "\r\n").getBytes(StandardCharsets.UTF_8);
            System.arraycopy(header, 0, result, pos, header.length);
            pos += header.length;
            System.arraycopy(arg, 0, result, pos, arg.length);
            pos += arg.length;
            result[pos++] = '\r';
            result[pos++] = '\n';
        }

        return result;
    }

    /**
     * Returns a human-readable representation of this command.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        for (byte[] arg : args) {
            sb.append(' ');
            String str = new String(arg, StandardCharsets.UTF_8);
            if (str.length() > 50) {
                sb.append(str.substring(0, 50)).append("...(").append(arg.length).append(" bytes)");
            } else if (str.contains(" ") || str.contains("\n")) {
                sb.append('"').append(str.replace("\"", "\\\"")).append('"');
            } else {
                sb.append(str);
            }
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Command that)) return false;
        if (!name.equals(that.name)) return false;
        if (args.size() != that.args.size()) return false;
        for (int i = 0; i < args.size(); i++) {
            if (!Arrays.equals(args.get(i), that.args.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        for (byte[] arg : args) {
            result = 31 * result + Arrays.hashCode(arg);
        }
        return result;
    }
}
