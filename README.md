##Spark Sentiment Analysis

This code base will demonstrate how to build basic sentiment analysis system with Apache Spark.

We will be doing data transformation using Scala and Apache Spark 2, 
and For the sentiments calculation, here we are using a dictionary called AFINN.

__AFINN is a dictionary which consists of 2500 words rated from +5 to -5 depending on their meaning.__

```scala
sbt run
```

**Output**

```text
(1,[8,trump walks into nato, a room full of progressivesocialists. warm welcome? not really. then he holds their bureaucratic feet to the fire.])
(2,[0,hehe my mom definitely just bought every copy of @teenvogue at the highway gas station. honored to be featured w so… https://t.co/oxdrtnfy2f])
(2,[11,the love of red knows no season! 😂])
(-1,[7,no. no it don't. https://t.co/g7opgvzok0])
(-2,[1,regarding nato: does anyone here want to send their kids to die for erdogan's dictatorial t… https://t.co/ewvaq0vjro])
(-3,[6,oh hell yeah])
(0,[2,❤️])
(0,[4,guyssssssssssssssssssssssssssssssss https://t.co/vwglu6yovk])
(0,[3,contacting @thinkspacehq rn because my heart... omg])
(0,[5,what about tom?!])
(0,[9,say it louder for the people in the back!!!])
```