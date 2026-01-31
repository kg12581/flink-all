package org.example.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data  // set get toString
//@AllArgsConstructor
@NoArgsConstructor
public class User {
    public String id;
    public String name;
    public int age;

    public User(String id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

}
