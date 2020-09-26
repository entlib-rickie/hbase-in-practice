package com.rickie.controller;

import com.rickie.model.Order;
import com.rickie.model.OrderQueryResponse;
import com.rickie.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("order")
public class OrderController {
    @Autowired
    private OrderService orderService;

    @RequestMapping(value = "query/orderId/{orderId}", method = RequestMethod.GET)
    public OrderQueryResponse queryByOrderId(@PathVariable(value="orderId") long orderId){
        OrderQueryResponse response = new OrderQueryResponse();
        if(orderId <=0) {
            response.setResult(false);
            response.setMessage("OrderId is missing or nonpositive");
            response.setOrder(null);
            return response;
        }
        Order order = orderService.getOrderByOrderId(orderId);
        if(order != null) {
            System.out.println(order.toString());
            response.setResult(true);
            response.setMessage("Got it");
            response.setOrder(order);
            return response;
        }
        response.setResult(false);
        response.setMessage("Cannot find it.");
        response.setOrder(null);
        return response;
    }

    @GetMapping("hello")
    public String hello() {
        return "hello world.";
    }
}
