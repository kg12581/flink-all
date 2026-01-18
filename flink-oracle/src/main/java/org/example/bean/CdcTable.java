package org.example.bean;

import java.sql.Timestamp;

public class CdcTable {
    public Integer id;
    public String name;
    public Timestamp createTime;
    public Integer status;

    public CdcTable() {}

    public CdcTable(Integer id, String name, Timestamp createTime, Integer status) {
        this.id = id;
        this.name = name;
        this.createTime = createTime;
        this.status = status;
    }

    @Override
    public String toString() {
        return "CdcTable{" +
                "id=" + id +
                ", userName='" + name + '\'' +
                ", createTime=" + createTime +
                ", status=" + status +
                '}';
    }

}
