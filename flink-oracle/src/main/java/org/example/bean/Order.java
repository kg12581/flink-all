package org.example.bean;

import java.sql.Timestamp;


public class Order {

    public Integer id;
    public String userName;
    public Double amount;
    public Timestamp createTime;

    public Order() {}

    public Order(Integer id, String userName, Double amount, Timestamp createTime) {
        this.id = id;
        this.userName = userName;
        this.amount = amount;
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", userName='" + userName + '\'' +
                ", amount=" + amount +
                ", createTime=" + createTime +
                '}';
    }
}


