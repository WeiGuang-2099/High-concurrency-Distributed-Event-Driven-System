package com.auction.gateway.it.support;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Stands in for one downstream service. It answers every request with 200 and records
 * the request line and headers exactly as they arrived, so a test can assert on what the
 * gateway forwarded rather than on what the gateway said.
 *
 * <p>Answering everything is deliberate. A stub that only knows the expected paths would
 * turn "the gateway forwarded something it should not have" into a 404 from the stub,
 * which looks the same as the gateway refusing it.
 */
public final class RecordingBackend {

    public record Received(String method, String uri, HttpHeaders headers) {

        public String requestLine() {
            return method + " " + uri;
        }
    }

    private final String name;
    private final List<Received> received = new CopyOnWriteArrayList<>();
    private final DisposableServer server;

    public RecordingBackend(String name) {
        this.name = name;
        this.server = HttpServer.create()
                .host("127.0.0.1")
                .port(0)
                .handle((request, response) -> {
                    received.add(new Received(request.method().name(), request.uri(),
                            request.requestHeaders().copy()));
                    if (request.requestHeaders().containsValue(
                            HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true)) {
                        return response.sendWebsocket((in, out) ->
                                out.sendString(Mono.just("ws-ok:" + name)).then(in.receive().then()));
                    }
                    return response.status(200)
                            .header(HttpHeaderNames.CONTENT_TYPE, "application/json")
                            .header("X-Backend", name)
                            .sendString(Mono.just("{\"code\":200,\"message\":\"OK\",\"data\":null}"));
                })
                .bindNow();
    }

    public String name() {
        return name;
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.port();
    }

    public List<Received> received() {
        return List.copyOf(received);
    }

    public List<String> requestLines() {
        return received.stream().map(Received::requestLine).toList();
    }

    public void reset() {
        received.clear();
    }
}
