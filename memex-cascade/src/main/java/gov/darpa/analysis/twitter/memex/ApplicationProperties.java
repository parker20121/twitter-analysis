package gov.darpa.analysis.twitter.memex;

import java.util.HashMap;
import org.apache.hadoop.hbase.HConstants;

/**
 *
 * @author Matt Parker
 */
public class ApplicationProperties extends HashMap<Object,Object> {
    
    public ApplicationProperties(){
        
        StringBuilder qorum = new StringBuilder();
        qorum.append("memex-zk01.xdata.data-tactics-corp.com,")
             .append("memex-zk02.xdata.data-tactics-corp.com,")
             .append("memex-zk03.xdata.data-tactics-corp.com,")
             .append("memex-zk04.xdata.data-tactics-corp.com,")
             .append("memex-zk05.xdata.data-tactics-corp.com");
        
        this.put("mapreduce.job.user.classpath.first", "true" );
        this.put("mapreduce.task.classpath.user.precedence", "true");
        this.put(HConstants.ZOOKEEPER_QUORUM, qorum.toString() );
        this.put(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");

    }
       
}

