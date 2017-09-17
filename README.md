# lcm-jms-helloworld

A simple example of using [Java Message Service (JMS)](http://download.oracle.com/otndocs/jcp/7195-jms-1.1-fr-spec-oth-JSpec/) for sending and receiving messages to/from a queue.

The project contains two classes, each with its own `main` method.

`HelloWorldActiveMQ.java` demonstrates how to use JMS with [ActiveMQ](http://activemq.apache.org/). Please refer to `JMS_HelloWorld.pdf` or `JMS_HelloWorld.docx` for more documentation on this example.

`HelloWorldSQS.java` demonstrates how to use JMS with AWS SQS (Amazon Web Services - Simple Queue Service). The `main` method first connects to SQS, then sends a text message, receives a text message, and finally closes the connection. To get the example running, two things have to be prepared:  
* Add your AWS API credentials, the access key and the secret key, to  `src/main/resources/aws.properties` (you may use `aws.properties.example` as a template)  
__Note: Do not rename__ `aws.properties.example` __to__ `aws.properties` __but rather create a new file named__ `aws.properties` __instead.__  
Otherwise, Git will no longer ignore the file, possibly resulting in your AWS credentials being committed and pushed and available to everyone.
* Create a SQS queue, e.g. using the AWS console, and then adjust `private static final String queueName = "..."` accordingly (`HelloWorldSQS.java, line 41`) 