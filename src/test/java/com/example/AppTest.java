package com.example;

import org.junit.Before;
import org.junit.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.RouterFunction;

public class AppTest {
    private WebTestClient testClient;

    @Before
    public void setUp() throws Exception {
//        RouterFunction<?> routes = App.routes();
//        this.testClient = WebTestClient.bindToRouterFunction(routes).build();
    }

    @Test
    public void testHello() throws Exception {
//        this.testClient.get()
//                .uri("/") //
//                .exchange() //
//                .expectStatus().isOk() //
//                .expectBody(String.class).isEqualTo("Hello World!");
    }
}
