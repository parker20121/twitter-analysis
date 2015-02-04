package gov.darpa.analysis.twitter.memex;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.planner.FlowPlanner;
import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTap;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

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
public class TwitterEbolaP1AfterTrailsCount {
    
    public static final String TABLE_NAME = "twitter-ebola-p1-after-trails";
    public static final String OUTPUT_HDFS_PATH = "/usr/mparker/" + TABLE_NAME;
    
    private static Logger log = Logger.getLogger(TwitterEbolaP1AfterTrailsCount.class);
    
    public static void main( String args[] ){
                         
        BasicConfigurator.configure();
        //HBaseConfiguration conf=new HBaseConfiguration();
        
        log.info("Starting cascade flow...");
        
            //Define where the data is coming from and going. Read data from HBase               
        log.debug("Adding taps...");
        Tap hbaseTap = new HBaseTap( TABLE_NAME, new HBaseScheme( new Fields("key"), "tileData", new Fields("avro") ), SinkMode.KEEP );
                      
        log.debug("Adding pipeline..");
        Pipe allRecords = new Pipe( "all_records" );
        Pipe groupAll = new GroupBy( allRecords, Fields.ALL );
        Pipe countRecords = new Every( groupAll, new Count(), new Fields("avro") );
               
            //Store the results in a text file on HDFS.               
        Scheme sinkScheme = new TextLine( new Fields( "record_count" ) );
        Tap summaryResults = new Hfs( sinkScheme, OUTPUT_HDFS_PATH );
                
        log.debug("Building flow...");
        Map<Object,Object> properties = new ApplicationProperties();
        
        AppProps.setApplicationJarClass(properties, TwitterEbolaP1AfterTrailsCount.class);        
        HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
        
        //Map<String, Tap> sources = new HashMap<String, Tap>();
        //sources.put( "hbase", hbaseTap );

        //Map<String, Tap> sinks = new HashMap<String, Tap>();
        //sinks.put( "summary", summaryResults );
        
        //Collection<Pipe> pipes = new ArrayList<Pipe>();
        //pipes.add( allRecords );
        //pipes.add( groupAll );
        //pipes.add( countRecords );
        
        log.debug("Running analysis....");
        Flow flow = flowConnector.connect( hbaseTap, summaryResults, countRecords );
        flow.complete();        
                
        //FlowDef flowDef = FlowDef.flowDef()
        //                         .setName("example_count")
        //                         .addSource(allRecords, hbaseTap)
        //                         .addSink(countRecords, summaryResults);
                
        
        //Flow analysis = flowConnector.connect(flowDef);
        //analysis.complete();
        
        
        log.info("Done.");
        
    }
    
}
