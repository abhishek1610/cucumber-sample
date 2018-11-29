package com.noradltd.demo.cucumberjvm.support;
import java.sql.*;
public class test {

    public int count( String table)
    {
        try{ int count=0;
            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/sakila","root","root");
//here sonoo is database name, root is username and password
            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery("select count(*) from world.city");
            while (rs.next()) count = rs.getInt(1);
            con.close();
            return count ;
        }catch(Exception e){ System.out.println(e);
        return 0;}
    }
        }




