package org.moerstw.hadoop;
import org.joda.time.LocalTime;
// only for java
public class TestHelloWord2 {
  
  public static class Greeter {
    public String sayHello() {
        return "Hello world!2";
    }
  }

  public static void main(String args[]) {
    LocalTime currentTime = new LocalTime();
    System.out.println("The current local time is: " + currentTime);
    Greeter greeter = new Greeter();
    System.out.println(greeter.sayHello());
  }  
}
