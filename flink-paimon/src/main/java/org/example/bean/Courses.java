package org.example.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Courses {
    private int course_id;
    private String course_name;
    private int teacher_id;
    private String category;
    private String difficulty;
}
