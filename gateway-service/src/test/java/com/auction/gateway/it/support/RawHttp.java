package com.auction.gateway.it.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * HTTP/1.1 over a plain socket, with the request-target written byte for byte.
 *
 * <p>The gateway tests need this because the usual clients tidy the path before it leaves
 * the JVM: {@code WebTestClient.uri(String)} collapses {@code //} and encodes {@code %}
 * again, so a test for {@code /api/tickets//reserve} written the obvious way sends
 * {@code /api/tickets/reserve} and passes without testing anything.
 */
public final class RawHttp {

    public record Response(int status, Map<String, String> headers, String body) {

        /** Header lookup by lower-case name; null when absent. */
        public String header(String name) {
            return headers.get(name.toLowerCase(Locale.ROOT));
        }
    }

    private RawHttp() {
    }

    public static Response send(int port, String method, String target, Map<String, String> headers) {
        boolean hasBody = method.equals("POST") || method.equals("PUT");
        StringBuilder request = new StringBuilder();
        request.append(method).append(' ').append(target).append(" HTTP/1.1\r\n");
        request.append("Host: localhost:").append(port).append("\r\n");
        headers.forEach((name, value) -> request.append(name).append(": ").append(value).append("\r\n"));
        if (hasBody) {
            request.append("Content-Type: application/json\r\n");
            request.append("Content-Length: 2\r\n");
        }
        // The response is read until the server closes the socket. A caller that sends its
        // own Connection header has to include "close" in it.
        if (headers.keySet().stream().noneMatch("Connection"::equalsIgnoreCase)) {
            request.append("Connection: close\r\n");
        }
        request.append("\r\n");
        if (hasBody) {
            request.append("{}");
        }

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(15_000);
            OutputStream out = socket.getOutputStream();
            out.write(request.toString().getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
            return parse(readAll(socket.getInputStream()), method, target);
        } catch (IOException e) {
            throw new UncheckedIOException(method + " " + target + " failed", e);
        }
    }

    private static String readAll(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int n;
        while ((n = in.read(chunk)) >= 0) {
            buffer.write(chunk, 0, n);
        }
        return buffer.toString(StandardCharsets.ISO_8859_1);
    }

    private static Response parse(String raw, String method, String target) {
        int headEnd = raw.indexOf("\r\n\r\n");
        String head = headEnd >= 0 ? raw.substring(0, headEnd) : raw;
        String body = headEnd >= 0 ? raw.substring(headEnd + 4) : "";
        String[] lines = head.split("\r\n");
        String[] statusLine = lines[0].split(" ");
        if (statusLine.length < 2) {
            throw new IllegalStateException("No HTTP status line for " + method + " " + target + ": '" + raw + "'");
        }
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon > 0) {
                headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT),
                        lines[i].substring(colon + 1).trim());
            }
        }
        return new Response(Integer.parseInt(statusLine[1]), headers, body);
    }
}
