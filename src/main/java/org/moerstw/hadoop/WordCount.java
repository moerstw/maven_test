package org.moerstw.hadoop;
import org.joda.time.LocalTime;

public class WordCount {
  
  public static class Greeter {
    public String sayHello() {
        return "Hello world!";
    }
  }

  public static void main(String args[]) {
    LocalTime currentTime = new LocalTime();
    System.out.println("The current local time is: " + currentTime);
    Greeter greeter = new Greeter();
    System.out.println(greeter.sayHello());
  }  
}
