/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <2583055>
 *
 */

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
//import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.


public class RecordsDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for artist's name and one for recordshop's name.
    private ResultSet outcome   = null;

	//JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public RecordsDatabaseService(Socket aSocket){
        
		//TO BE COMPLETED
		this.serviceSocket = aSocket;
        this.start();
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For artist
        this.requestStr[1] = ""; //For recordshop
		
		String tmp = "";
        try {

			//TO BE COMPLETED
			InputStream input = serviceSocket.getInputStream();
            InputStreamReader reader = new InputStreamReader(input);
            StringBuffer myBuffer = new StringBuffer();
            char myChar;
            while (true){
                myChar = (char) reader.read();
                if (myChar == '#'){
                    break;
                }else{
                    myBuffer.append(myChar);
                }
            }
            this.requestStr = myBuffer.toString().split(";");
         }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		
		this.outcome = null;
		
		String sql = "SELECT title,label,genre,rrp,COUNT(recordcopy.recordid) FROM record,artist,recordshop,recordcopy WHERE artist.lastname = ? AND recordshop.city = ? AND artist.artistid = record.artistid AND recordcopy.recordid = record.recordid AND recordshop.recordshopid = recordcopy.recordshopid GROUP BY title,label,genre,rrp HAVING COUNT(recordcopy.recordid) > 0;"; //TO BE COMPLETED- Update this line as needed.
		
		
		try {
			//Connet to the database
			//TO BE COMPLETED
            Class.forName("org.postgresql.Driver");
			Connection con = DriverManager.getConnection(URL,USERNAME,PASSWORD);
			//Make the query
			//TO BE COMPLETED
			PreparedStatement stat = con.prepareStatement(sql);
            stat.setString(1,this.requestStr[0]);
            stat.setString(2,this.requestStr[1]);

            //Process query
			//TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set
            ResultSet rs = stat.executeQuery();
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crs = aFactory.createCachedRowSet();
            crs.populate(rs);
            while (crs.next()){
                System.out.println(crs.getString(1) + " | "+crs.getString(2) + " | "+crs.getString(3) + " | "+crs.getString(4) + " | "+crs.getString(5));
            }
            this.outcome = crs;
			//Clean up
			//TO BE COMPLETED
			rs.close();;
            stat.close();
            con.close();
		} catch (Exception e)
		{ System.out.println(e.getMessage()); }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
			//Return outcome
			//TO BE COMPLETED
            OutputStream output = serviceSocket.getOutputStream();
			ObjectOutputStream osw = new ObjectOutputStream(output);

            osw.writeObject(this.outcome);
            osw.flush();
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);
            
			//Terminating connection of the service socket
			//TO BE COMPLETED
			serviceSocket.close();
			
        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
