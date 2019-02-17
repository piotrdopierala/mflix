package mflix;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);

    String welcomeMessage ="MFLIX movie application.";
    System.out.println(welcomeMessage);
  }
}
