package com.fantasy.clash.framework.cassandra;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.fantasy.clash.framework.configuration.Configurator;
import com.fantasy.clash.framework.utils.StringUtils;

@Configuration
public class CassandraConfiguration {
Logger logger  = LoggerFactory.getLogger(CassandraConfiguration.class);
  
  @Autowired
  private Configurator config;
  
  private Map<String, Session> aliasToCassandraSessionMap;  
  private Map<String, Cluster> aliasToCassandraClusterMap;
  
  @PostConstruct
  public void init() {
    aliasToCassandraSessionMap = new HashMap<String, Session>();
    aliasToCassandraClusterMap = new HashMap<String,Cluster>();
  }
  
  public Session getSession(String alias) {
    
    if(this.aliasToCassandraSessionMap.containsKey(alias)) {
      return this.aliasToCassandraSessionMap.get(alias);
    }
    String keyspaceName = config.getString("cassandra." + alias + ".keyspacename");
    Session session;
    try {
      Cluster cluster = getCassandraCluster(alias);
      session = cluster.connect(keyspaceName);
      this.aliasToCassandraSessionMap.put(alias, session);
      return session;
    }catch(Exception e) {
      logger.error("Error while creating cassandra session", e);
    }
   
    
    return null;
  }
  
  private Cluster getCassandraCluster(String alias) throws Exception{
    
    if(this.aliasToCassandraClusterMap.containsKey(alias)) {
      return this.aliasToCassandraClusterMap.get(alias);
    }
    
    String userName = config.getString("cassandra." + alias + ".username");
    String password = config.getString("cassandra." + alias + ".password");
    String contactPoints = config.getString("cassandra." + alias + ".contactpoints");
    Integer port = config.getInt("cassandra." + alias + ".port",9042);
    
    Cluster cluster;
   
    try {
      
      CassandraClusterFactoryBean cassandraClusterFactoryBean = new CassandraClusterFactoryBean();
      
      PoolingOptions poolingOptions = new PoolingOptions();
      poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL,200);
      poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE,200);
      cassandraClusterFactoryBean.setPoolingOptions(poolingOptions);
      
      QueryOptions queryOptions = new QueryOptions();
      queryOptions.setFetchSize(config.getInt("cassandra."+alias+".fetch.size",2000));
      cassandraClusterFactoryBean.setQueryOptions(queryOptions);
      
      cassandraClusterFactoryBean.setContactPoints(contactPoints);
      cassandraClusterFactoryBean.setPort(port);
      
      
      if(StringUtils.isNotNullAndEmpty(userName)) {
        cassandraClusterFactoryBean.setUsername(userName);
     }
     
     if(StringUtils.isNotNullAndEmpty(password)) {
       cassandraClusterFactoryBean.setPassword(password);
     }
     
     cassandraClusterFactoryBean.setMetricsEnabled(false);
     cassandraClusterFactoryBean.afterPropertiesSet();
     
     cluster = cassandraClusterFactoryBean.getObject();
     
     this.aliasToCassandraClusterMap.put(alias, cluster);
     
     return cluster;
    }catch(Exception e) {
      logger.error("Error while creating cassandra cluster", e);
    }
    
    return null;
  }
  
  public void destroy(String alias) {
    try {
        
        if (this.aliasToCassandraSessionMap.containsKey(alias)) {
            Session session = this.aliasToCassandraSessionMap.get(alias);
            if(session!=null) {
              session.close();
              this.aliasToCassandraSessionMap.remove(alias);
              logger.info("Cassandra "+alias+" session destroyed");
            }
        }
        
        if (this.aliasToCassandraClusterMap.containsKey(alias)) {
            Cluster cluster = this.aliasToCassandraClusterMap.get(alias);
            if(cluster!=null) {
              cluster.close();
              this.aliasToCassandraClusterMap.remove(alias);
              logger.info("Cassandra "+alias+" cluster destroyed");
            }
        }
        
    } catch (Exception e) {
        logger.error("Cassandra session destroy operation failed due to {}",e);
    }
}

}
