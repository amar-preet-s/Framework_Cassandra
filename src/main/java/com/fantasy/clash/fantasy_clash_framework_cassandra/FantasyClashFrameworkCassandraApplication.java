package com.fantasy.clash.fantasy_clash_framework_cassandra;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication
@EnableAutoConfiguration(
    exclude = {DataSourceAutoConfiguration.class,CassandraAutoConfiguration.class})
public class FantasyClashFrameworkCassandraApplication {

	public static void main(String[] args) {
		SpringApplication.run(FantasyClashFrameworkCassandraApplication.class, args);
	}

}
