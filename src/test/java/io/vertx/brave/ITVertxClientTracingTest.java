package io.vertx.brave;

import brave.http.ITHttpClient;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import org.junit.BeforeClass;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ITVertxClientTracingTest extends ITHttpClient<HttpClient> {

  @BeforeClass
  public static void before() {
    VertxTracing.instrument();
  }
  private Vertx vertx;

  @Override
  protected HttpClient newClient(int port) {
    if (vertx == null) {
      VertxTracing tracing = VertxTracing.create(httpTracing);
      vertx = Vertx.vertx(
        new VertxOptions()
          .setMetricsOptions(tracing.metricsOptions()))
        .exceptionHandler(tracing.exceptionHandler());
    }
    return vertx.createHttpClient(new HttpClientOptions().setDefaultPort(port));
  }

  @Override
  protected void closeClient(HttpClient client) throws Exception {
    client.close();
  }

  @Override
  protected void get(HttpClient client, String pathIncludingQuery) throws Exception {
    CompletableFuture<Void> fut = new CompletableFuture<>();
    client.get(url(pathIncludingQuery), resp -> {
      resp.exceptionHandler(fut::completeExceptionally);
      resp.endHandler(v -> {
        fut.complete(null);
      });
    }).exceptionHandler(fut::completeExceptionally).end();
    fut.get(10, TimeUnit.SECONDS);
  }

  @Override
  protected void post(HttpClient client, String pathIncludingQuery, String body) throws Exception {
    CompletableFuture<Void> fut = new CompletableFuture<>();
    client.post(url(pathIncludingQuery), resp -> {
      resp.exceptionHandler(fut::completeExceptionally);
      resp.endHandler(v -> {
        fut.complete(null);
      });
    }).exceptionHandler(fut::completeExceptionally).end(body);
    fut.get(10, TimeUnit.SECONDS);
  }

  @Override
  protected void getAsync(HttpClient client, String pathIncludingQuery) throws Exception {
    client.get(url(pathIncludingQuery), resp -> { }).end();
  }
}
