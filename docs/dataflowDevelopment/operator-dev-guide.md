Operator Development Guide
==========================

#Contents#
1. [Overview](#overview)
2. [A Simple Operator](#a-simple-operator)
3. [A Committing Operator](#a-committing-operator)
4. [Filtering Data and Choosing a Sink to Write to](#filtering-data-and-choosing-a-sink-to-write-to)
5. [Enhancing Data](#enhancing-data)

#Overview
An Operator is how Scribengin processes data.  When data comes in through a source, Scribengin can perform any arbitrary operation on it (filtering, transforming, enhancing), and then spit it out to any sink.  This guide will walk you through that.


#A Simple Operator

This operator is super simple.  The process() method gets passed in the DataStreamOperatorContext and a Message.  This operator simply takes the Message it receives (from the source) and writes it to all available sinks.  The Operator will automatically handle committing.

```java
import java.util.Set;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorContext;

//Your Operator must extend DataStreamOperator
public class SimpleOperator extends DataStreamOperator {
  
  //Your Operator must at least override the method process()
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {

    //Get all sinks
    Set<String> sink = ctx.getAvailableOutputs();
    //For each sink, write the record
    for(String selSink : sink) {
      ctx.write(selSink, record);
    }
  }
}
```


#A Committing Operator

How do we improve?  

Well, ctx.commit() is an expensive operation.  This is when Scribengin has a chunk of messages to be moved to the sink and finalizes on it.  Various tracking actions must be accomplished during a commit().

Therefor, we would benefit from not running commit() every time we get a message, but instead after a certain number of messages are received, or even after a timeout is hit.

```java

public class CommittingOperator extends DataStreamOperator {
  private int count = 0 ;
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {
    Set<String> sink = ctx.getAvailableOutputs();
    for(String selSink : sink) {
      ctx.write(selSink, record);
    }
    
    //Commit every 1000 messages
    count++;
    if(count > 0 && count % 10000 == 0) {
      ctx.commit();
    }
  }
}
```


#Filtering Data and Choosing a Sink to Write to
What if we have multiple sinks to write to?  Suppose you want to write to a different Kafka topic depending on the data being passed in, or you wanted to write to different partitions in HDFS?

In this example, we'll demonstrate how to choose which sink to use.

This example assumes you have set up two separate and available sinks.  This will be done when setting up your dataflow.  For information on how to setup your dataflow and data sink names, refer to [the Dataflow Development API Guide](wireDataflowDev.md#creating-a-splitterdatastreamoperator)

```java
public class FilteringOperator extends DataStreamOperator {
  
  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {


    //Here is where we'll analyze our data
    //First, lets convert our record into a String
    String sMessage= new String(record.getData()) ;

    //If that message begins with data...
    if(sMessage.startsWith("DATA")){
      //Then we'll write it to a sink called "data-sink"
      ctx.write("data-sink", record)
    }
    else{
      //Otherwise we'll write it to a sink called "everything-else-sink"
      ctx.write("everything-else-sink", record)
    }

  }
}
```


#Enhancing Data

Lets's say we want to do something else with our data - say transform it from AVRO to JSON, or filter for junk messages, or anything else before we write to a sink.  

In this next example, we'll be enhancing our data by adding a prefix to all our data being passed in


```java
import java.io.ByteArrayOutputStream;

public class EnhancingOperator extends DataStreamOperator {
  private static byte[] enhance = new String("Scribengin ROCKS! ").getBytes();
  private ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );

  @Override
  public void process(DataStreamOperatorContext ctx, Message record) throws Exception {

    //Reset our ByteArrayOutputStream so we can reuse it
    outputStream.reset();

    //We'll use a ByteArrayOutputStream to 
    //  concatenate our static enhancement string with
    //  what's already been passed in via the Message
    outputStream.write( enhance );
    outputStream.write( record.getData() );

    //Set the record's data to our newly enhanced data
    record.setData(outputStream.toByteArray());

    //Write the modified record to all sinks
    Set<String> sink = ctx.getAvailableOutputs();
    for(String selSink : sink) {
      ctx.write(selSink, record);
    }
    
  }
}
```