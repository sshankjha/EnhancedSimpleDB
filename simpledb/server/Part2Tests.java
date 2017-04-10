package simpledb.server;

import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.tx.recovery.LogRecordIterator;
import simpledb.tx.recovery.RecoveryMgr;

public class Part2Tests {

	public static void testCase1() {
		System.out.println("\n\nRecovery test case 1");
		//Test case to check forward iteration
		Block blk1 = new Block("filename1", 1);
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
        
		try {
			basicBufferMgr.pin(blk1);
		} catch (BufferAbortException e) {
			e.printStackTrace();
		}

		RecoveryMgr rm = new RecoveryMgr(333);
				
		int lsn1 = rm.setInt(basicBufferMgr.getMapping(blk1), 4, 10);
		int lsn2 = rm.setInt(basicBufferMgr.getMapping(blk1), 4, 20);
		int lsn3 = rm.setInt(basicBufferMgr.getMapping(blk1), 4, 30);
		basicBufferMgr.flushAll(333);
		
		LogRecordIterator it = new LogRecordIterator();
		while (it.hasNextFwd()) {
			System.out.println(it.nextFwd());
		}
		
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));
	}

	
	public static void testCase2(){
		System.out.println("\n\nRECOVERY test case 2");
		//Test case to check with multiple recovery managers, all committed transactions
		Block blk1 = new Block("filename1", 1);
		Block blk2 = new Block("filename2", 2);
		Block blk3 = new Block("filename3", 3);
		Block blk4 = new Block("filename4", 4);
		Block blk5 = new Block("filename5", 5);
		Block blk6 = new Block("filename6", 6);
		
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		try {
			basicBufferMgr.pin(blk1);
			basicBufferMgr.pin(blk2);
			basicBufferMgr.pin(blk3);
			basicBufferMgr.pin(blk4);
			basicBufferMgr.pin(blk5);
			basicBufferMgr.pin(blk6);
		} catch (BufferAbortException e) {
			e.printStackTrace();
		}
		
		RecoveryMgr rm11 = new RecoveryMgr(11);
		int lsn11 = rm11.setInt(basicBufferMgr.getMapping(blk1), 4, 3);
		int lsn12 = rm11.setString(basicBufferMgr.getMapping(blk2), 4, "test1");
		rm11.commit();		
		
		RecoveryMgr rm22 = new RecoveryMgr(22);
		int lsn21 = rm22.setInt(basicBufferMgr.getMapping(blk3), 4,12);
		int lsn22 = rm22.setString(basicBufferMgr.getMapping(blk4), 4, "test2");
		rm22.commit();
				
		RecoveryMgr rm33 = new RecoveryMgr(33);
		int lsn31 = rm33.setInt(basicBufferMgr.getMapping(blk5), 4, 398);
		int lsn32 = rm33.setString(basicBufferMgr.getMapping(blk6), 4, "test3");
		rm33.commit();
				
		rm11.doRecover();
		LogRecordIterator it = new LogRecordIterator();
		while (it.hasNextFwd()) {
			System.out.println(it.nextFwd());
		}
		
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk2));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk3));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk4));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk5));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));
	}
	
	
	public static void testCase3(){
		System.out.println("\n\nRecovery test case 3");
		//Test case to check with multiple recovery managers, no committed transactions
		Block blk1 = new Block("filename1", 1);
		Block blk2 = new Block("filename2", 2);
		Block blk3 = new Block("filename3", 3);
		Block blk4 = new Block("filename4", 4);
		Block blk5 = new Block("filename5", 5);
		Block blk6 = new Block("filename6", 6);
		
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		try {
			basicBufferMgr.pin(blk1);
			basicBufferMgr.pin(blk2);
			basicBufferMgr.pin(blk3);
			basicBufferMgr.pin(blk4);
			basicBufferMgr.pin(blk5);
			basicBufferMgr.pin(blk6);
		} catch (BufferAbortException e) {
			e.printStackTrace();
		}
		
		RecoveryMgr rm11 = new RecoveryMgr(11);
		int lsn11 = rm11.setInt(basicBufferMgr.getMapping(blk1), 4, 7);
		int lsn12 = rm11.setString(basicBufferMgr.getMapping(blk2), 4, "test12");
				
		RecoveryMgr rm22 = new RecoveryMgr(22);
		int lsn21 = rm22.setInt(basicBufferMgr.getMapping(blk3), 4, 67);
		int lsn22 = rm22.setString(basicBufferMgr.getMapping(blk4), 4, "test22");
				
		RecoveryMgr rm33 = new RecoveryMgr(33);
		int lsn31 = rm33.setInt(basicBufferMgr.getMapping(blk5), 4, 834);
		int lsn32 = rm33.setString(basicBufferMgr.getMapping(blk6), 4, "test32");
				
		rm11.doRecover();
		LogRecordIterator it = new LogRecordIterator();
		while (it.hasNextFwd()) {
			System.out.println(it.nextFwd());
		}
		
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk2));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk3));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk4));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk5));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));
	}
	
	
	public static void testCase4(){
		System.out.println("\n\nRecovery test case 4");
		//Test case to check with 3 recovery managers, 1 committed transaction, 2 uncommitted transactions
		Block blk1 = new Block("filename1", 1);
		Block blk2 = new Block("filename2", 2);
		Block blk3 = new Block("filename3", 3);
		Block blk4 = new Block("filename4", 4);
		Block blk5 = new Block("filename5", 5);
		Block blk6 = new Block("filename6", 6);
		
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		try {
			basicBufferMgr.pin(blk1);
			basicBufferMgr.pin(blk2);
			basicBufferMgr.pin(blk3);
			basicBufferMgr.pin(blk4);
			basicBufferMgr.pin(blk5);
			basicBufferMgr.pin(blk6);
		} catch (BufferAbortException e) {
			e.printStackTrace();
		}
		
		RecoveryMgr rm11 = new RecoveryMgr(11);
		int lsn11 = rm11.setInt(basicBufferMgr.getMapping(blk1), 4, 9);
		int lsn12 = rm11.setString(basicBufferMgr.getMapping(blk2), 4, "test13");
				
		RecoveryMgr rm22 = new RecoveryMgr(22);
		int lsn21 = rm22.setInt(basicBufferMgr.getMapping(blk3), 4, 98);
		int lsn22 = rm22.setString(basicBufferMgr.getMapping(blk4), 4, "test23");
		rm22.commit();
				
		RecoveryMgr rm33 = new RecoveryMgr(33);
		int lsn31 = rm33.setInt(basicBufferMgr.getMapping(blk5), 4, 738);
		int lsn32 = rm33.setString(basicBufferMgr.getMapping(blk6), 4, "test33");
				
		rm11.doRecover();
		LogRecordIterator it = new LogRecordIterator();
		while (it.hasNextFwd()) {
			System.out.println(it.nextFwd());
		}
		
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk2));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk3));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk4));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk5));
		basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));
	}
}
