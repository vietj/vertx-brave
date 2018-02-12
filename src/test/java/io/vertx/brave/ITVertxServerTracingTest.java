package io.vertx.brave;

import brave.Span;
import brave.http.ITHttpServer;
import brave.propagation.ExtraFieldPropagation;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class ITVertxServerTracingTest extends ITHttpServer {

  Vertx vertx;
  HttpServer server;
  volatile int port;
  volatile boolean sameSpan;

  @BeforeClass public static void before() {
    VertxTracing.instrument();
  }

  @Override protected void init() throws Exception {
    stop();

    VertxTracing tracing = VertxTracing.create(httpTracing);

    vertx = Vertx.vertx(
      new VertxOptions()
        .setMetricsOptions(tracing.metricsOptions()))
      .exceptionHandler(tracing.exceptionHandler());

    Handler<HttpServerRequest> requestHandler = req -> {
      switch (req.path()) {
        case "/foo":
          req.response().end("bar");
          break;
        case "/async":
          Span current = tracing.tracer.currentSpan();
          vertx.runOnContext(v -> {
            Span trace = tracing.tracer.newTrace();
            tracing.tracer.withSpanInScope(trace);
          });
          vertx.runOnContext(v -> {
            sameSpan = current.context() == tracing.tracer.currentSpan().context();
            req.response().end("bar");
          });
          break;
        case "/extras":
          req.response().end(ExtraFieldPropagation.get(EXTRA_KEY));
          break;
        case "/badrequest":
          req.response().setStatusCode(400).end("bar");
          break;
        case "/child":
          httpTracing.tracing().tracer().nextSpan().name("child").start().finish();
          req.response().setStatusCode(400).end("happy");
          break;
        case "/exception":
          throw new RuntimeException();
        case "/exceptionAsync":
          req.endHandler(v -> {
            throw new RuntimeException();
          });
      }
    };

    server = vertx.createHttpServer(new HttpServerOptions().setPort(0).setHost("localhost"));

    CountDownLatch latch = new CountDownLatch(1);
    server.requestHandler(requestHandler).listen(async -> {
      port = async.result().actualPort();
      latch.countDown();
    });

    assertThat(latch.await(10, TimeUnit.SECONDS))
      .withFailMessage("server didn't start")
      .isTrue();
  }

  @Test
  public void usesExistingTraceId() throws Exception {
    super.usesExistingTraceId();
  }

  @Override
  protected String url(String path) {
    return "http://127.0.0.1:" + port + path;
  }

  @After
  public void stop() throws Exception {
    if (server != null) {
      CountDownLatch latch = new CountDownLatch(1);
      server.close(ar -> {
        latch.countDown();
      });
      latch.await(10, TimeUnit.SECONDS);
    }
    if (vertx != null) {
      CountDownLatch latch = new CountDownLatch(1);
      vertx.close(ar -> {
        latch.countDown();
      });
      latch.await(10, TimeUnit.SECONDS);
      vertx = null;
    }
  }

  @Test
  @Override
  public void async() throws Exception {
    sameSpan = false;
    super.async();
    assertTrue(sameSpan);
  }
}
