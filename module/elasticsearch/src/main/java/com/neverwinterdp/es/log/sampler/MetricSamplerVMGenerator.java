package com.neverwinterdp.es.log.sampler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MetricSamplerVMGenerator implements Runnable {
  private String optionsAsString;
  private String mainClass;
  private String[] arguments;

  MetricSamplerVMGenerator(String optionsAsString, String mainClass, String[] arguments) {
    this.optionsAsString = optionsAsString;
    this.mainClass = mainClass;
    this.arguments = arguments;
  }

  public void startNewJavaProcess() throws IOException, InterruptedException {

    ProcessBuilder processBuilder = createProcess();
    System.out.println("Starting new vm  " + optionsAsString + "; args= "
        + Arrays.toString(arguments));
    Process process = processBuilder.start();
    output(process.getInputStream());
    int errCode = process.waitFor();
    if(errCode==1){
      System.out.println("Command executed, any errors? " + (errCode == 0 ? "No" : "Yes"));
      output(process.getErrorStream());
    }
  }

  private ProcessBuilder createProcess() {
    String jvm = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    String[] options = optionsAsString.split(" ");
    List<String> command = new ArrayList<String>();
    command.add(jvm);
    command.addAll(Arrays.asList(options));
    command.add(mainClass);
    command.addAll(Arrays.asList(arguments));
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    Map<String, String> environment = processBuilder.environment();
    environment.put("CLASSPATH", classpath);
    
    return processBuilder;
  }

  private void output(InputStream inputStream) throws IOException {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(inputStream));
      String line = null;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }
    } finally {
      br.close();
    }
  }


  @Override
  public void run() {
    try {
      startNewJavaProcess();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
