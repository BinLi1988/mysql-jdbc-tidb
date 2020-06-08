package com.example.mysqljdbcloadbalance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

@RestController
public class TaskController {

    @Autowired
    private DataSource dataSource;

    @GetMapping("/task/query/{count}/{seconds}")
    public String query(@PathVariable("count") int count,@PathVariable("seconds") int seconds){

        Assert.isTrue(count > 0,"'count' should be great than 0");
        Assert.isTrue(seconds > 0,"'seconds' should be great than 0");

        while(count -- >0){
            new Thread(()->{
                long start = System.currentTimeMillis();
                while(true){
                    try(Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("select * from seq limit 1")){
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                    if((System.currentTimeMillis() - start)/1000 > seconds){
                        break;
                    }
                }
            }).start();
        }
        return "OK";
    }

    @GetMapping("/task/insert/{count}/{seconds}/{sleepSeconds}")
    public String insert(@PathVariable("count") int count,@PathVariable("seconds")int seconds,@PathVariable("sleepSeconds") int sleepSeconds) {

        Assert.isTrue(count > 0, "'count' should be great than 0");
        Assert.isTrue(seconds > 0, "'seconds' should be great than 0");
        Assert.isTrue(sleepSeconds > 0, "'sleepSeconds' should be great than 0");

        while (count-- > 0) {
            new Thread(() -> {
                long start = System.currentTimeMillis();
                String value = null;
                while (true) {
                    try (Connection connection = dataSource.getConnection();
                         Statement statement = connection.createStatement();) {
                        connection.setAutoCommit(false);
                        value = UUID.randomUUID().toString();
                        System.out.println("=======>"+value);
                        boolean inserted = statement.execute("insert into seq values ('" + value + "')");
                        try {
                            Thread.sleep(sleepSeconds * 1000);
                        } catch (InterruptedException e) {
                            System.err.println("The value : " + value +" can not be inserted");
                            e.printStackTrace();
                        }
                        connection.commit();
                        System.out.println(Thread.currentThread().getName()+" => " + value);
                    } catch (SQLException throwables) {
                        System.err.println("The value : " + value +" can not be inserted");
                        throwables.printStackTrace();
                    }
                    if ((System.currentTimeMillis() - start) / 1000 > seconds) {
                        break;
                    }
                }
            }).start();
        }

        return "OK";
    }

}
