package gov.darpa.analysis.twitter.memex;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTap;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.MinBy;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import java.util.Properties;

/**
 *
 * <code>
 * MEMEX HBase Tables
 * ==================
 * /backup/latest/memex_sotera/rubmap_twitter
 * /data/memex_sotera/rubmap_twitter
 * /hbase/archive/data/default/twitter-ebola-p1-after-trails
 * /hbase/archive/data/default/twitter-ebola-p1-all-trails
 * /hbase/archive/data/default/twitter-ebola-p1-before-trails
 * /hbase/archive/data/default/twitter-ebola-p1-keyword-top-users-full.keywords
 * /hbase/archive/data/default/twitter-ebola-p1-keyword-top-users-full.users
 * /hbase/data/default/twitter-ebola-daily-after-trails
 * /hbase/data/default/twitter-ebola-daily-all-trails
 * /hbase/data/default/twitter-ebola-daily-before-trails
 * /hbase/data/default/twitter-ebola-daily-top-users-keywords
 * /hbase/data/default/twitter-ebola-daily-top-users-users
 * /hbase/data/default/twitter-ebola-p1-after-trail
 * /hbase/data/default/twitter-ebola-p1-heatmap
 * /hbase/data/default/twitter-ebola-p1-keyword-top-users-full.keywords
 * /hbase/data/default/twitter-ebola-p1-keyword-top-users-full.users
 * /hbase/data/default/twitter-ebola-p1-n5-all-trails
 * </code>
 * 
 * @author <a href="mailto:matthew.parker@l3-com.com">Matt Parker</a>
 */
public class TwitterEbolaP1AfterTrails {
    
    public static void main( String args[] ){
        
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, TwitterEbolaP1AfterTrails.class);
        
        HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
               
            //Define where the data is coming from and going.
            //Read data from HBase
        Fields keyFields = new Fields("metaData");
        String familyName = "";
        Fields valueFields = new Fields("tileData");
        String tableName = "twitter-ebola-p1-after-trails";        
        Tap hbaseTable = new HBaseTap( tableName, new HBaseScheme( keyFields, familyName, valueFields ) );
              
            //Store the results in a text file on HDFS. 
            //The console may be just as easy.
        Scheme sourceScheme = new TextLine( new Fields( "line" ) );
        Tap summaryResults = new Hfs(sourceScheme, tableName);
        
            //For each Twitter HBase record, extract the value field and convert the 
            //data to an Avro record that can be read by the pipe processes below.
        Pipe avroData = new Avro();
        
            //Stream AVRO data from HBase table, and examine the tweet's date.
        Pipe analysisPipe = new Pipe("analysis", avroData);
        Pipe startDate = new MinBy(analysisPipe,);
        Pipe endDate = new MaxBy(analysisPipe);
        
        FlowDef flowDef = FlowDef.flowDef()
                                 .setName("date_range")
                                 .addSource( startDate, hbaseTable )
                                 .addTailSink( endDate, hbaseTable );
        
        Flow analysis = flowConnector.connect(flowDef);
        analysis.writeDOT("dot/wt.dot");
        
    }
}
