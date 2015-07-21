package com.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import  com.datastax.driver.core.ResultSet;


/**
 * Created by fadi on 7/20/15.
 */
public class CassandraSupport {

    String serverurl = "127.0.0.1";



    public CassandraSupport() {

    }

    Cluster cluster = Cluster.builder().addContactPoint(serverurl).build();
    Session session = cluster.connect();

    protected void createKeyspace(String keyspace) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +  " WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1}");
        session.execute("USE " + keyspace);

    }

    //example for creating a type: create type testjson.employee (firstname text, lastname text, email text, duty text );

    protected void createUserdefinedtype(String keyspace, String type, String fields) {

        session.execute("CREATE TYPE " + keyspace +"." + type + " (" + fields + ")");
    }


    /*examples to creates table with map collection data type and user defined user type:
    CREATE TABLE testjson (ID text PRIMARY KEY, json map<text,text>);
    create table company (id uuid PRIMARY KEY, employees frozen <employee>);*/

    protected void createTable(String table, String columns) {

        session.execute("create table " + table +" (" + columns + ")" );
    }


    //example for loading data into user defined type:
    // INSERT INTO testjson.company (id, employees) VALUES (123e4567-e89b-12d3-a456-426655441111, {firstname: 'John', lastname: 'Smith', email: 'John@gmail.com', duty: 'manager'});

    protected  void loadData(String table, String columns, String values) {
        session.execute("INSERT INTO " + table +" (" + columns +")" +" VALUES" +" (" + values +")");

    }


    // * = all data
    // dot notation: column.field to query a single field from a table using user defined data
    protected  void queryData(String select, String table) {

        ResultSet query = session.execute("SELECT " + select + " FROM " + table);
        System.out.println("\nQueried data:\n" + query.all());


    }

    protected void deleteTable(String table) {

        session.execute("DROP TABLE " + table);
    }

    protected void deleteKeyspace(String keyspace) {

         session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
    }

    public static void main (String args[]) {

        CassandraSupport instance = new CassandraSupport();

        //instance.deleteTable("fadi");

        instance.deleteKeyspace("test");

        instance.createKeyspace("test");

        instance.createUserdefinedtype("test", "employee", "firstname text, lastname text, address text, phone text");

        instance.createTable("company", "id uuid PRIMARY KEY, employees frozen <employee>");

        //INSERT INTO testjson.company (id, employees) VALUES (123e4567-e89b-12d3-a456-426655441111, {firstname: 'John', lastname: 'Smith', email: 'John@gmail.com', duty: 'manager'});
        //instance.loadData("fadi", "id, employees", "one, {firstname: 'John', lastname: 'Smith', address: 'Main st Fremont CA', phone: '123456789'}");



        instance.loadData("company", "id, employees", "123e4567-e89b-12d3-a456-426655441111, {firstname: 'John', lastname: 'Smith', address: 'Main st Fremont CA', phone: '123456789'}");
        instance.loadData("company", "id, employees", "123e4567-e89b-12d3-a456-426655442222, {firstname: 'Rick', lastname: 'Smith', address: 'Main st Dublin CA', phone: '987654321'}");

        //SELECT employees FROM testjson.company WHERE id=123e4567-e89b-12d3-a456-426655441111;
        //instance.queryData("employees", "company");

        instance.queryData("employees", "company");


    }




}
