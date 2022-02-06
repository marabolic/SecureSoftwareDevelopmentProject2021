package com.zuehlke.securesoftwaredevelopment.domain;

public class Customer {
    Integer id;
    String username;
    String password;

    public Customer(Integer id, String username) {
        this.id = id;
        this.username = username;
    }

    public Customer(Integer id, String username, String password) {
        this.id = id;
        this.username = username;
        this.password = password;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
