package org.example.bean;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class CdcTable {
    public Integer id;
    public String name;
    public LocalDateTime createTime;
    public Integer status;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public CdcTable() {}

    public CdcTable(Integer id, String name, LocalDateTime createTime, Integer status) {
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
