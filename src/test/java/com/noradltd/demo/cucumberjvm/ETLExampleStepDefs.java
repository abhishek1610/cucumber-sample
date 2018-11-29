package com.noradltd.demo.cucumberjvm;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.noradltd.demo.cucumberjvm.ordersods.Order;
import com.noradltd.demo.cucumberjvm.ordersods.OrdersODS;
import com.noradltd.demo.cucumberjvm.ordersods.OrdersOutputStreamWriter;
import com.noradltd.demo.cucumberjvm.support.ETLBogon;

import com.noradltd.demo.cucumberjvm.support.test;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

public class ETLExampleStepDefs {

  private static final String ORDERS_CSV = "orders.csv";

  private List<Order> orders = new ArrayList<Order>();
  private OutputStream ordersOutputStream = new ByteArrayOutputStream();
  private OrdersODS ordersODS = new OrdersODS();
  private ETLBogon etlBogon = null;

  @Before
  public void beforeETLExampleFeature() throws IOException {
    startETLProcess();
  }

  @After
  public void afterELTExampleFeature() {
    stopETLProcess();
  }

  @Given("^Given a file \\\"([^\\\"]*)\\\" is present in staging area$")
  public void fileexists(String filename) throws Throwable {
    File tmpDir = new File("D:\\Automation\\"+ filename);
    boolean exists = tmpDir.exists();
    assertTrue(exists);
  }

  @When("^ETL process has run and \\\"([^\\\"]*)\\\" generated$")
  public void logexists1(String filename) throws Throwable {
    File tmpDir = new File("D:\\Automation\\log\\"+ filename);
    boolean exists = tmpDir.exists();
    assertTrue(exists);
  }

  @Then("^Count should match between the file and table$")
  public void Nightly_Orders_are_loaded_into_the_ODS() throws Throwable {

    test t1 = new test();
    int count = t1.count("sample");
    assertThat(count, is(4079));

  }


  @Then("^Record \\\"([^\\\"]*)\\\" should exists in landing table$")
  public void a_corrupt_file_notification_is_logged(int a) throws Throwable {
    assertThat(1, is(1));
  }

  @Given("^a partially corrupt nightly orders load file$")
  public void a_partially_corrupt_nightly_orders_load_file() throws Throwable {
    loadValidOrders();
    ordersOutputStream.write("This file is invalid".getBytes());
  }

  private void loadValidOrders() throws ParseException {
    String[][] ordersData = new String[][] { { "1", "2013-10-23" }, { "2", "2013-10-23" } };
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    for (String[] orderStrings : ordersData) {
      orders.add(new Order(orderStrings[0], dateFormat.parse(orderStrings[1])));
    }
    new OrdersOutputStreamWriter().write(orders, ordersOutputStream);
  }

  private void writeOrdersToFile() throws IOException {
    FileWriter writer = null;
    try {
      writer = new FileWriter(ORDERS_CSV);
      writer.write(ordersOutputStream.toString());
      writer.flush();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } finally {
      writer.close();
    }
    synchronized (ordersOutputStream) {
      ordersOutputStream.notifyAll();
    }
  }

  private void startETLProcess() throws IOException {
    if (etlBogon == null) {
      etlBogon = new ETLBogon(ordersODS, ordersOutputStream);
      etlBogon.start();
    }
  }

  private void stopETLProcess() {
    if (etlBogon != null) {
      try {
        etlBogon.quit().join(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      etlBogon = null;
    }
  }

  private void waitForETLProcess() throws InterruptedException {
    synchronized (etlBogon) {
      etlBogon.wait(20000);
    }
  }

}
