package com.example.demo;

public class User {

    private String name;
    private int age;
    private String status ="";

    User(){

    }

    User(String name, int age){
        this.age = age;
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status += status;
    }
}
