package linpac;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.mail.BodyPart;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.Store;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.sun.mail.util.MailSSLSocketFactory;

public class MailParserTask extends TimerTask{
	
	private Logger logger = null ;
	private String sMailHostAddress = "" ;
	private String sMailHostLogin = "" ;
	private String sMailHostPassword = "" ;
	private String sMailAttachmentFolder = "" ;
	private String sMailAttachmentUrl = "" ;
	
	// connection data 
	private Properties prop = null ;	

	public MailParserTask(Properties oprop, Timer timer, Logger log) throws Exception{
		prop = oprop;
		//oTimer = timer ;
		logger = log ;
	}

	public void run() {
		Connection pConn = null ;
		Store store = null ;
		Folder emailFolder = null ;
		Message[] messages = null;
		logger.info("Starting thread...");
		try{
			// establish db connection
			try {
				logger.info("Establish database connection...");
				pConn = getDBConnection() ;
			} 
			catch (Exception e) {
				logger.error("Unable to establish database connection, will try later in " + prop.getProperty("thread.time") + "s", e);
				return ; // try later if better
			}
						
			try {
				logger.info("reading email connection from database...");
				getDbSetting(pConn, "I5M_IFT_DATA", "LIN_SET_MAIL_PARSER_");
			} 
			catch (Exception e) {
				logger.error("Unable to read email connection from database", e);
				return ; // try later if better
			}	
			
			// get server connection
			try {
				logger.info("Setting email connection...");
				store = getStore() ;
				emailFolder = store.getFolder("INBOX");
				emailFolder.open(Folder.READ_ONLY);
				messages = emailFolder.getMessages();
			} 
			catch (Exception e) {
				logger.error("Unable to get a mail session, will try later in " + prop.getProperty("thread.time") + "s", e);
				return ; // try later if better
			}		
					
			// get db connection
			try {
				logger.info("Setting database connection...");
				pConn = getDBConnection() ;
				int N = 100;
				logger.info("(" + messages.length + ") messages found (only last 100 processed)");
				if(messages.length < N) {
					for ( Message message : messages ) { 
						insertMAil(pConn, message);
					}
				}
				else{
					for (int i = messages.length - N; i < messages.length; i++) {
						Message message = messages[i];
						insertMAil(pConn, message);
					}
				}
				emailFolder.close(false);
			} 
			catch (Exception e) {
				logger.error("Unable to open a database connection, will try later in " + prop.getProperty("thread.time") + "s", e);
				return ; // try later if better
			}
			
			// process
			if(pConn != null){
				pConn.commit(); 
				pConn.close(); 
			}
			if(store != null)store.close();
		}
		catch (Exception ex) {			 
			logger.error("Thread interrupted", ex);	
			return ;
		}
		logger.info("Thread stopped.");
	}

	private Connection getDBConnection() throws Exception{
		Class.forName(prop.getProperty("database.class.driver.class"));
		Connection connection = DriverManager.getConnection(prop.getProperty("database.connection.string"), 
														prop.getProperty("database.connection.login"), 
														prop.getProperty("database.connection.password"));
		return connection ;
	}
	
	private void getDbSetting(Connection connection, String sDataset, String sSetting) throws Exception{
		Statement statement = null;
		ResultSet rs = null;
		try {
			statement = connection.createStatement();
			rs = statement.executeQuery("select a.name, a.string_value from common_dataset b, globalsettings a "+
										" where b.onb = a.dataset AND a.onb>0 AND a.NAME like '" + sSetting + "%' AND b.name = '" + sDataset + "'");
			while(rs.next()){
				if(rs.getString("name").equals("LIN_SET_MAIL_PARSER_ADDRESS")){
					sMailHostAddress = rs.getString("string_value") ;
				}
				else if(rs.getString("name").equals("LIN_SET_MAIL_PARSER_LOGIN")){
					sMailHostLogin = rs.getString("string_value") ;
				}
				else if(rs.getString("name").equals("LIN_SET_MAIL_PARSER_PASSWORD")){
					sMailHostPassword = rs.getString("string_value") ;
				}
				else if(rs.getString("name").equals("LIN_SET_MAIL_PARSER_ATT_FOLDER")){
					sMailAttachmentFolder = rs.getString("string_value") ;
				}
				else if(rs.getString("name").equals("LIN_SET_MAIL_PARSER_ATT_URL")){
					sMailAttachmentUrl = rs.getString("string_value") ;
				}				
			}
			//logger.info("Host mail address ... " + sMailHostAddress);
			//logger.info("login mail address ... " + sMailHostLogin);
			//logger.info("password mail address ... " + sMailHostPassword);
		}
		catch(Exception e){
			if(rs != null)rs.close();
			if(statement != null)statement.close();
			throw e ;
		}
	}

	private Store getStore() throws Exception{
		Properties properties = new Properties();		
		properties.setProperty("mail.store.protocol", "pop3");
		MailSSLSocketFactory sf = new MailSSLSocketFactory();
		sf.setTrustAllHosts(true); 
		properties.put("mail.pop3.ssl.trust", "*");
		properties.put("mail.pop3.ssl.socketFactory", sf);	
		Session emailSession = Session.getDefaultInstance(properties, null);
		// create the POP3 store object and connect with the pop server
		Store store = emailSession.getStore();
		store.connect(sMailHostAddress, sMailHostLogin, sMailHostPassword);
		return store ;
	}

	public void insertMAil(Connection conn, Message message) {
		String SQL = "";
		String subject = "";
		String sender = "";
		String recipients = "";
		String  sentDate = "" ;
		String content = "";
		String references = "";
		String replyTo = "";
		String mailID = null;
		int mailAttachment = 0;
		List<File> attachments = new ArrayList<File>();
		PreparedStatement oStmt = null ;
		try {
			String[] idHeaders = message.getHeader("Message-ID");			
			if (message.getHeader("References") != null){
				String[] idReferences = message.getHeader("References");
				references = idReferences[0]; 
				if(references.indexOf("<") != -1)
					references = references.substring(references.indexOf("<") + 1, references.indexOf(">"));
			}			
			if ( message.getHeader("In-Reply-To") != null){
				String[] idReply = message.getHeader("In-Reply-To");
				replyTo = idReply[0]; 
				if(replyTo.indexOf("<") != -1)
					replyTo = replyTo.substring(replyTo.indexOf("<") + 1, replyTo.indexOf(">"));
			}
			if (idHeaders != null && idHeaders.length > 0) {
				mailID = idHeaders[0];
			} 
			else {
				logger.error("\"Message-ID\" header not found during message conversion; trying \"Message-Id\".");
				idHeaders = message.getHeader("Message-Id");
				if (idHeaders != null && idHeaders.length > 0)
					mailID = idHeaders[0];
				else {
					logger.error("No message ID headers found during message conversion; generating an artificial one.");
				}
			}
			if(mailID.indexOf("<") != -1)
				mailID = mailID.substring(mailID.indexOf("<") + 1, mailID.indexOf(">"));
			if (mailExist(conn, mailID) == 0 && mailReplyExist(conn, mailID) == 0) {
				logger.info("new email (" + mailID + ")");
				subject = message.getSubject();
				if (message.getSentDate() != null) {
					sentDate = message.getSentDate().toString();
					sentDate = sentDate.substring(4,7).toUpperCase()+ sentDate.substring(7,11) + sentDate.substring(sentDate.length() - 4,sentDate.length()) + " " +sentDate.substring(11,20);
				}
				sender = message.getFrom()[0].toString();
				for (int j = 0; j < message.getAllRecipients().length; j++) {
					String newRecipient = message.getAllRecipients()[j].toString().replaceAll("'", "");
					recipients = recipients + newRecipient + ",";					
				}
				recipients = recipients.substring(0, recipients.length() - 1);
				if (message.getContent() instanceof String) {
					content = message.getContent().toString();
				} 
				else if (message.getContent() instanceof Multipart) {
					Multipart multipart = (Multipart) message.getContent();
					File file = null;
					for (int j = 0; j < multipart.getCount(); j++) {
						BodyPart bodyPart = multipart.getBodyPart(j);
						if (bodyPart.getContentType().contains("text/plain")
								|| bodyPart.getContentType().contains("text/html")
								) {
							content = content + "\n" + bodyPart.getContent();
						}
						if (!Part.ATTACHMENT.equalsIgnoreCase(bodyPart.getDisposition())
								&& !StringUtils.isNotBlank(bodyPart.getFileName())) {
							mailAttachment = 0;
							continue;
						}
						InputStream is = bodyPart.getInputStream();
						mailAttachment = 1;
						try {
							//logger.error("Creating an attachment " + prop.getProperty("mail.attachment.folder.download") + bodyPart.getFileName());
							file = new File(sMailAttachmentFolder + bodyPart.getFileName());
							FileOutputStream fos = new FileOutputStream(file);
							byte[] buf = new byte[4096];
							int bytesRead;
							while ((bytesRead = is.read(buf)) != -1) {
								fos.write(buf, 0, bytesRead);
							}
							fos.close();
							attachments.add(file);
						} 
						catch (IOException e) {
							logger.error("Error when write file to server (" + sMailAttachmentFolder + bodyPart.getFileName() + ")", e);
						}
					}

				}
				if(mailAttachment == 1){
					logger.info("attachment for mail (" + mailID + ")");
					byte[] buffer = new byte[1024];
					try{
						int length;
						String query;						
						File file = null;	
						//logger.error("Zip attachment " + prop.getProperty("mail.attachment.folder.download") + mailID + ".zip");
						file = new File(sMailAttachmentFolder + mailID + ".zip");
						FileOutputStream fos = new FileOutputStream(file);
						ZipOutputStream zos = new ZipOutputStream(fos);
						for(File filee:attachments){
							ZipEntry ze= new ZipEntry(filee.getName());		
							zos.putNextEntry(ze);
							FileInputStream in = new FileInputStream(filee.getPath());		
							int len;
							while ((len = in.read(buffer)) > 0) {
								zos.write(buffer, 0, len);
							}
							in.close();
						}
						zos.closeEntry();
						//remember close it
						zos.close();		
						length = (int) file.length();
						query = ("insert into AttachmentFile VALUES(?,?,?,?,?)");
						oStmt = conn.prepareStatement(query);
						oStmt.setString(1, mailID);
						//logger.error("file.getName() " + file.getName());
						String name = file.getName().substring(0,file.getName().lastIndexOf('@'));
						name = name.replace('$', ' ');
						oStmt.setString(2, file.getName().substring(file.getName().lastIndexOf('.') + 1));
						System.out.println(name);
						oStmt.setString(3, name);		
						oStmt.setInt(4, length);
						oStmt.setString(5, sMailAttachmentUrl + mailID +".zip");
						oStmt.executeUpdate();
						oStmt.close(); 
					}
					catch(IOException ex){
						logger.error("Error when insert attachment in database (" + prop.getProperty("mail.attachment.folder.download") + mailID + ".zip" + ")", ex);
					}
				}				
				if((replyTo != "" && replyTo != null) && (mailExist(conn, replyTo) != 0 || mailReplyExist(conn, replyTo) != 0)){
					logger.info("Replay to mail..."); 
					SQL = "insert into MailReply (MAILID, MAILSUBJECT, MAILSENDER, MAILRECIPIENTS, MAILCONTENT, MAILSENTDATE, PUBLISHED, REPLYTO, MAILREFERENCES, MAILATTACHMENT) "+
												" values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " ;
					PreparedStatement stmt = conn.prepareStatement(SQL);
					stmt.setString(1, mailID);
					stmt.setString(2, subject);
					stmt.setString(3, sender);
					stmt.setString(4, recipients);
					stmt.setString(5, content);
					stmt.setString(6, sentDate);
					stmt.setInt(7, 0);
					stmt.setString(8, replyTo);
					stmt.setString(9, references);
					stmt.setInt(10, mailAttachment);
					stmt.executeUpdate() ;
					stmt.close();
				}
				else {
					logger.info("Mail...");
					SQL = "insert into Mail (MAILID, MAILSUBJECT, MAILSENDER, MAILRECIPIENTS, MAILCONTENT, MAILSENTDATE, PUBLISHED, MAILATTACHMENT) "+
							" values (?, ?, ?, ?, ?, ?, ?, ?) " ; 
					PreparedStatement stmt = conn.prepareStatement(SQL);
					stmt.setString(1, mailID);
					stmt.setString(2, subject);
					stmt.setString(3, sender);
					stmt.setString(4, recipients);
					stmt.setString(5, content);
					stmt.setString(6, sentDate);
					stmt.setInt(7, 0);
					stmt.setInt(8, mailAttachment);
					stmt.executeUpdate() ;
					stmt.close();				
				}
			}
		}
		catch (Exception e) {
			logger.error("Unexpected error occured", e);
		}
	}

	public int mailExist(Connection connection, String mailID) throws SQLException {
		Statement statement = null;
		ResultSet rs = null;
		int rowCount = -1;
		try {
			statement = connection.createStatement();
			rs = statement.executeQuery("SELECT COUNT(*) FROM Mail where mailId = '" + mailID + "'");
			rs.next();
			rowCount = rs.getInt(1);
		} finally {
			rs.close();
			statement.close();
		}
		return rowCount;
	}

	public int mailReplyExist(Connection connection, String mailID) throws SQLException {
		Statement statement = null;
		ResultSet rs = null;
		int rowCount = -1;
		try {
			statement = connection.createStatement();
			rs = statement.executeQuery("SELECT COUNT(*) FROM MailReply where mailId = '" + mailID + "'");
			rs.next();
			rowCount = rs.getInt(1);
		} finally {
			rs.close();
			statement.close();
		}
		return rowCount;
	}
}
