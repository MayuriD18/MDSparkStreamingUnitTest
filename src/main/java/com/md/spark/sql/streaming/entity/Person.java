package com.md.spark.sql.streaming.entity;

import scala.Serializable;

public class Person implements Serializable {


    private String name;
    private String address;
    private String department;


    public static Person newInstance(String name, String address, String department){
        Person instance = new Person();

        instance.setName(name);
        instance.setAddress(address);
        instance.setDepartment(department);

        return instance;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

}
