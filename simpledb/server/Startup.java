package simpledb.server;

import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.remote.*;

import java.rmi.registry.*;

public class Startup {
	public static void main(String args[]) throws Exception {
		// configure and initialize the database
		// SimpleDB.init(args[0]);
		SimpleDB.init("simpleDB");

		// create a registry specific for the server on the default port
		Registry reg = LocateRegistry.createRegistry(1099);
	
		// and post the server entry in it
		RemoteDriver d = new RemoteDriverImpl();
		reg.rebind("simpledb", d);
		


		System.out.println("database server ready");

		
		/*In order to test functionality of FIFO implementation, run the following test cases.
		 *The expected output can be seen in the README file as well as the file Part1Tests.java*/
		
		Part1Tests.FifoTestCase1();
		Part1Tests.FifoTestCase2();
		Part1Tests.FifoTestCase3();
		Part1Tests.FifoTestCase4();
		Part1Tests.FifoTestCase5();
		
		//Tests for recovery
		Part2Tests.testCase1();
		Part2Tests.testCase2();
		Part2Tests.testCase3();
		Part2Tests.testCase4();
	
		
	}
	

}
