# twitter-analysis
Cascading applications to analyze twitter data in HBase.


To compile the Cascading classes into a single jar with dependencies

    mvn clean compile assembly:single

To run the Cascading application on Hadoop, execute the following:

    hadoop jar memex-cascading-jar-with-dependencies-1.0.jar gov.darpa.analysis.twitter.memex.TwitterEbolaP1AfterTrailsCount