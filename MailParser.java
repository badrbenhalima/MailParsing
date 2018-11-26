package linpac;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Timer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MailParser {

	final static Logger logger = LogManager.getLogger(MailParser.class);
	
	public static void main(String[] args) {
		try{
			if(args == null || args.length != 4){
				System.out.println("wrong number of argument, expected 4 : database type, connstring, user, password");
				System.exit(-1); 
			}
			String sDbType = args[0] ; 
			
			Properties prop = new Properties() ;
			String filename = "config.properties";
			InputStream inputStream = MailParser.class.getClassLoader().getResourceAsStream(filename);
			if (inputStream == null) {
				System.out.println("Config file not found");
				System.exit(-1); 
			}
			prop.load(inputStream);
			
			if(sDbType.toUpperCase().equals("ORACLE"))	
				prop.setProperty("database.class.driver.class", "oracle.jdbc.driver.OracleDriver") ;
			else if(sDbType.toUpperCase().equals("postgresql".toUpperCase()))
				prop.setProperty("database.class.driver.class", "org.postgresql.Driver") ;
			else{
				System.out.println("Unhandled database type, expected oracle or postgresql");
				System.exit(-1); 
			}
			prop.setProperty("database.connection.string", args[1]) ;
			prop.setProperty("database.connection.login", args[2]) ;
			prop.setProperty("database.connection.password", args[3]) ;
			
			String process_pid = ManagementFactory.getRuntimeMXBean().getName() ;
			logger.info("Starting Timer for process mail parser (" + process_pid + ")");
			Timer timer; 
			timer = new Timer(); 
			timer.schedule(new MailParserTask(prop, timer, logger), 1000, Long.parseLong(prop.getProperty("thread.time")));
		}
		catch(Exception e){
			System.out.println("Email Parser, exit with error :: " + e.toString());
			System.exit(-1); 
		}
	}

}
