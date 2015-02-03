package gov.darpa.analysis.twitter.memex;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTap;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
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
        
            //Define where the data is coming from and going. Read data from HBase               
        log.debug("Adding taps...");
        Tap hbaseTap = new HBaseTap( TABLE_NAME, new HBaseScheme( new Fields("key"), "tileData", new Fields("avro") ), SinkMode.KEEP );
              
            //Store the results in a text file on HDFS. 
            //The console may be just as easy.        
        Scheme sourceScheme = new TextLine( new Fields( "record_count" ) );
        Tap summaryResults = new Hfs( sourceScheme, OUTPUT_HDFS_PATH );
        
        System.out.println("Adding pipeline..");
        Pipe allRecords = new Pipe( "all_records" );
        Pipe countRecords = new Every( allRecords, new Fields("tileData"), new Count() );
                      
        System.out.println("Building flow...");
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, TwitterEbolaP1AfterTrailsCount.class);        
        HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
        Flow analysis = flowConnector.connect( hbaseTap, summaryResults, allRecords );
        
        System.out.println("Running analysis....");
        analysis.complete();
        
        System.out.println("Done.");
        
    }
    
}
