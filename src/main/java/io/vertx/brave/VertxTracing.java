package io.vertx.brave;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.http.*;
import brave.propagation.CurrentTraceContext;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.AbstractNioChannel;
import io.netty.util.concurrent.OrderedEventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.metrics.impl.DummyVertxMetrics;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.core.spi.metrics.HttpClientMetrics;
import io.vertx.core.spi.metrics.HttpServerMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatchers;
import zipkin2.Endpoint;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class VertxTracing {

  private static final ConcurrentHashMap<OrderedEventExecutor, VertxTracing> EVENT_LOOP_MAP = new ConcurrentHashMap<>();

  private static final Field m = getField();

  private static Field getField() {
    try {
      Field m = SingleThreadEventExecutor.class.getDeclaredField("taskQueue");
      m.setAccessible(true);
      return m;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public static class Interceptor {
    public static boolean intercept(@This SingleThreadEventExecutor thiz,
                                    @Argument(0) Runnable task) throws Exception {
      Queue<Runnable> o = (Queue<Runnable>) m.get(thiz);

      VertxTracing vertxTracing = EVENT_LOOP_MAP.get(thiz);
      if (vertxTracing != null) {
        Tracing tracing = vertxTracing.httpTracing.tracing();
        CurrentTraceContext cc = tracing.currentTraceContext();

        // ServerRequestContext ctx = current.get();

        System.out.println("cc.get() = " + cc.get());

        return o.offer(cc.wrap(() -> {
//        if (ctx != null) {

//          TraceContext tc = cc.get();
//          current.set(ctx);
//          .withSpanInScope(ctx.span);
//        }
          task.run();
        }));
      } else {
        return o.offer(task);
      }
    }
  }

  public static class BootstrapInterceptor {

    public static ChannelFuture intercept(@This Bootstrap thiz, @SuperCall Callable<ChannelFuture> m) {
      return null;
    }

  }

  public static void instrument() {
    ByteBuddyAgent.install();

    new AgentBuilder.Default()
      .disableClassFormatChanges()
      .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
      .type(ElementMatchers.is(SingleThreadEventExecutor.class))
      .transform((builder, typeDescription, classLoader, module) -> builder
        .method(named("offerTask")).intercept(MethodDelegation
          .to(Interceptor.class)))
      .type(ElementMatchers.is(Bootstrap.class))
      .transform((builder, typeDescription, classLoader, module) -> builder
        .method(named("doResolveAndConnect")).intercept(MethodDelegation
          .to(BootstrapInterceptor.class)))
      .installOnByteBuddyAgent();
  }

  static final Propagation.Getter<HttpServerRequest, String> GETTER = new Propagation.Getter<HttpServerRequest, String>() {
    @Override public String get(HttpServerRequest carrier, String key) {
      return carrier.getHeader(key);
    }

    @Override public String toString() {
      return "HttpServerRequest::getHeader";
    }
  };

  static final Propagation.Setter<HttpClientRequest, String> SETTER =
    new Propagation.Setter<HttpClientRequest, String>() {
      @Override public void put(HttpClientRequest carrier, String key, String value) {
        carrier.headers().set(key, value);
      }
      @Override public String toString() {
        return "HttpClientRequest::putHeader";
      }
    };

  static final HttpServerAdapter<HttpServerRequest, HttpServerResponse> SERVER_ADAPTER =
    new HttpServerAdapter<HttpServerRequest, HttpServerResponse>() {
      @Override public String method(HttpServerRequest request) {
        return request.method().name();
      }

      @Override public String url(HttpServerRequest request) {
        return request.absoluteURI();
      }

      @Override public String requestHeader(HttpServerRequest request, String name) {
        return request.headers().get(name);
      }

      @Override public Integer statusCode(HttpServerResponse response) {
        return response.getStatusCode();
      }

      @Override
      public boolean parseClientAddress(HttpServerRequest req, Endpoint.Builder builder) {
        if (super.parseClientAddress(req, builder)) return true;
        SocketAddress addr = req.remoteAddress();
        if (builder.parseIp(addr.host())) {
          builder.port(addr.port());
          return true;
        }
        return false;
      }
    };

  static final HttpClientAdapter<HttpClientRequest, HttpClientResponse> CLIENT_ADAPTER =
    new HttpClientAdapter<HttpClientRequest, HttpClientResponse>() {

      @Override
      public String method(HttpClientRequest request) {
        HttpMethod method = request.method();
        return method == HttpMethod.OTHER ? request.getRawMethod() : method.name();
      }

      @Override
      public String url(HttpClientRequest request) {
        return request.absoluteURI();
      }

      @Override
      public String requestHeader(HttpClientRequest request, String name) {
        return request.headers().get(name);
      }

      @Override
      public Integer statusCode(HttpClientResponse response) {
        return response.statusCode();
      }
    };

  public static VertxTracing create(Tracing tracing) {
    return create(HttpTracing.create(tracing));
  }

  public static VertxTracing create(HttpTracing httpTracing) {
    return new VertxTracing(httpTracing);
  }

//  static final ThreadLocal<ServerRequestContext> current = new ThreadLocal<>();
  final Tracer tracer;
  final HttpTracing httpTracing;
  final HttpServerHandler<HttpServerRequest, HttpServerResponse> serverHandler;
  final HttpClientHandler<HttpClientRequest, HttpClientResponse> clientHandler;
  final TraceContext.Extractor<HttpServerRequest> extractor;

  public VertxTracing(HttpTracing httpTracing) {
    tracer = httpTracing.tracing().tracer();
    this.httpTracing = httpTracing;
    serverHandler = HttpServerHandler.create(httpTracing, SERVER_ADAPTER);
    clientHandler = HttpClientHandler.create(httpTracing, CLIENT_ADAPTER);
    extractor = httpTracing.tracing().propagation().extractor(GETTER);
  }

  public MetricsOptions metricsOptions() {
    return new MetricsOptions().setEnabled(true).setFactory(factory);
  }

  public Handler<Throwable> exceptionHandler() {
    return err -> {
      /*
      ServerRequestContext ctx = current.get();
      if (ctx != null && !ctx.ended) {
        ctx.ended = true;
        serverHandler.handleSend(null, err, ctx.span);
        ctx.request.response().setStatusCode(500).end();
      }
      */
    };
  }

  private static class ServerRequestContext {
    private final HttpServerRequest request;
    private final Span span;
    private boolean ended;
    ServerRequestContext(HttpServerRequest request, Span span) {
      this.request = request;
      this.span = span;
    }
  }

  private static class ClientRequestContext {
    private final Span span;
    ClientRequestContext(Span span) {
      this.span = span;
    }
  }

  private final VertxMetricsFactory factory = new VertxMetricsFactory() {
    @Override
    public VertxMetrics metrics(Vertx vertx, VertxOptions vertxOptions) {

      int size = vertxOptions.getEventLoopPoolSize();
      for (int i = 0;i < size;i++) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        EVENT_LOOP_MAP.put(ctx.nettyEventLoop(), VertxTracing.this);
      }

      return new DummyVertxMetrics() {
        @Override
        public HttpServerMetrics createMetrics(HttpServer server, SocketAddress localAddress, HttpServerOptions options) {
          return new HttpServerMetrics<ServerRequestContext, Void, Void>() {

            @Override
            public ServerRequestContext requestBegin(Void socketMetric, HttpServerRequest request) {
              Span span = serverHandler.handleReceive(extractor, request);
              tracer.withSpanInScope(span);
              ServerRequestContext ctx = new ServerRequestContext(request, span);
              // current.set(ctx);
              return ctx;
            }

            public void responseEnd(ServerRequestContext requestMetric, HttpServerResponse response) {
              if (requestMetric != null && !requestMetric.ended) {
                requestMetric.ended = true;
                serverHandler.handleSend(response, null, requestMetric.span);
              }
            }

            public void requestReset(ServerRequestContext requestMetric) { }
            public ServerRequestContext responsePushed(Void socketMetric, HttpMethod method, String uri, HttpServerResponse response) { return null; }
            public Void upgrade(ServerRequestContext requestMetric, ServerWebSocket serverWebSocket) { return null; }
            public Void connected(Void socketMetric, ServerWebSocket serverWebSocket) { return null; }
            public void disconnected(Void serverWebSocketMetric) { }
            public Void connected(SocketAddress remoteAddress, String remoteName) { return null; }
            public void disconnected(Void socketMetric, SocketAddress remoteAddress) { }
            public void bytesRead(Void socketMetric, SocketAddress remoteAddress, long numberOfBytes) { }
            public void bytesWritten(Void socketMetric, SocketAddress remoteAddress, long numberOfBytes) { }
            public void exceptionOccurred(Void socketMetric, SocketAddress remoteAddress, Throwable t) { }
            public boolean isEnabled() { return true; }
            public void close() { }
          };
        }

        @Override
        public HttpClientMetrics createMetrics(HttpClient client, HttpClientOptions options) {
          return new HttpClientMetrics<ClientRequestContext, Void, Void, Void, Void>() {
            @Override
            public ClientRequestContext requestBegin(Void endpointMetric, Void socketMetric, SocketAddress localAddress, SocketAddress remoteAddress, HttpClientRequest request) {

              CurrentTraceContext current = httpTracing.tracing().currentTraceContext();

              TraceContext parent = current.get();

              System.out.println("PARENT " + parent);

              Span span;
              try (CurrentTraceContext.Scope scope = current.newScope(parent)) {
                span = clientHandler.handleSend(httpTracing.tracing().propagation().injector(SETTER), request
                  , request);
              }
              return new ClientRequestContext(span);
            }

            public void responseEnd(ClientRequestContext requestMetric, HttpClientResponse response) {
              if (requestMetric != null) {
                clientHandler.handleReceive(response, null, requestMetric.span);
              }
            }

            public void requestEnd(ClientRequestContext requestMetric) { }
            public void responseBegin(ClientRequestContext requestMetric, HttpClientResponse response) { }
            public void requestReset(ClientRequestContext requestMetric) { }
            public Void createEndpoint(String host, int port, int maxPoolSize) { return null; }
            public void closeEndpoint(String host, int port, Void endpointMetric) { }
            public Void enqueueRequest(Void endpointMetric) { return null; }
            public void dequeueRequest(Void endpointMetric, Void taskMetric) { }
            public void endpointConnected(Void endpointMetric, Void socketMetric) { }
            public void endpointDisconnected(Void endpointMetric, Void socketMetric) { }
            public ClientRequestContext responsePushed(Void endpointMetric, Void socketMetric, SocketAddress localAddress, SocketAddress remoteAddress, HttpClientRequest request) { return null; }
            public Void connected(Void endpointMetric, Void socketMetric, WebSocket webSocket) { return null; }
            public void disconnected(Void webSocketMetric) { }
            public Void connected(SocketAddress remoteAddress, String remoteName) { return null; }
            public void disconnected(Void socketMetric, SocketAddress remoteAddress) { }
            public void bytesRead(Void socketMetric, SocketAddress remoteAddress, long numberOfBytes) { }
            public void bytesWritten(Void socketMetric, SocketAddress remoteAddress, long numberOfBytes) { }
            public void exceptionOccurred(Void socketMetric, SocketAddress remoteAddress, Throwable t) { }
            public boolean isEnabled() { return true; }
            public void close() { }
          };
        }
      };
    }
  };
}
