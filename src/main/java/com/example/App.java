package com.example;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.function.server.HandlerStrategies;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.server.HttpServer;
import reactor.rabbitmq.*;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Optional;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;
import static reactor.rabbitmq.ExchangeSpecification.exchange;
import static reactor.rabbitmq.ResourcesSpecification.queue;

public class App {
    private static Logger log = LoggerFactory.getLogger(App.class);

    static RouterFunction<ServerResponse> routes() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();

        // Create a sender
        SenderOptions senderOptions = new SenderOptions()
                .connectionFactory(connectionFactory)
                .resourceManagementScheduler(Schedulers.elastic());
        Sender sender = ReactorRabbitMq.createSender(senderOptions);
        // Create a receiver
        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.elastic());
        Receiver receiver = ReactorRabbitMq.createReceiver(receiverOptions);

        // Declare and bind Exchange/Queue
        Mono<AMQP.Queue.BindOk> declareAndBind = sender.declare(exchange("demo").type("topic"))
                .then(sender.declare(queue("demo.sink").durable(true)))
                .then(sender.bind(ResourcesSpecification.binding("demo", "#", "demo.sink")))
                .doOnSuccess(x -> log.info("Exchange and queue declared and bound."))
                .doOnError(e -> {
                    log.error("Connection failed.", e);
                    System.exit(1);
                }).cache();

        // Build stream pipelines
        Flux<OutboundMessage> inbound =
                Flux.interval(Duration.ofMillis(100))
                        .map(i -> new OutboundMessage("demo", "#", ("Hello " + i).getBytes()));
        Flux<String> send = declareAndBind.thenMany(sender.sendWithPublishConfirms(inbound)
                .map(r -> new String(r.getOutboundMessage().getBody()) + " => " + r.isAck())
                .log("send"));
        Flux<String> receive = declareAndBind.thenMany(Flux.interval(Duration.ofMillis(200))
                .onBackpressureDrop()
                .zipWith(receiver.consumeAutoAck("demo.sink")
                        .map(d -> new String(d.getBody())))
                .map(Tuple2::getT2)
                .log("receive"));

        // Define route mappings
        return route(GET("/send"), req -> ok().contentType(MediaType.TEXT_EVENT_STREAM).body(send, String.class)) //
                .andRoute(GET("/receive"), req -> ok().contentType(MediaType.TEXT_EVENT_STREAM).body(receive, String.class));
    }

    public static void main(String[] args) throws Exception {
        long begin = System.currentTimeMillis();
        int port = Optional.ofNullable(System.getenv("PORT")) //
                .map(Integer::parseInt) //
                .orElse(8080);
        HttpServer httpServer = HttpServer.create().host("0.0.0.0").port(port);
        httpServer.route(routes -> {
            HttpHandler httpHandler = RouterFunctions.toHttpHandler(
                    App.routes(), HandlerStrategies.builder().build());
            routes.route(x -> true, new ReactorHttpHandlerAdapter(httpHandler));
        }).bindUntilJavaShutdown(Duration.ofSeconds(3), disposableServer -> {
            long elapsed = System.currentTimeMillis() - begin;
            LoggerFactory.getLogger(App.class).info("Started in {} seconds",
                    elapsed / 1000.0);
        });
    }
}
