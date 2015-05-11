# Basic http web log analysis using apache spark and scala

##How to use?

Example

```
* Example command to run:
 spark-submit
   --class "com.cloudwick.spark.loganalysis.HitsPerHour"
   --master local[4]
    target/scala-2.10/scala-2.10/myspark_2.10-1.0.jar
    /Users/arun/mylogpath/mock_apache_pool-1-thread-1.data

```
## Classes and details

Package 

```
com.cloudwick.spark.loganalysis

```
| Class        | Description        
| ------------- |:-------------:| 
| col 3 is      | right-aligned |
| col 2 is      | centered      |
| HitsPerHour | The HitsPerHour finds the hits happend in a hourly basis     | 
| HitsPerUrl | The HitsPerUrl gives the number of hits per URL     | 
| LogSizeAggregator  | The LogSizeAggregator takes in an apache access log file and computes min, max and avg of content size of the log.    | 
| StatusCounter | The StatusCounter aggragate the log messages based on the status code    | 
| MsgSizeVsHits | The MsgSizeVsHits calculate the message size and aggregate according to that     | 
| TopEndpoints | The TopEndpoints return the top 10 end points.  | 
| TopIpaddresses | The TopIpaddresses return top 10 IP Addresses.  | 
|ApacheAccessLog | Log Parser|



