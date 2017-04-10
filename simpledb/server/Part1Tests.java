package simpledb.server;

import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;

public class Part1Tests {

	/*Test Case 1 Expected Output
	Execution: pin(1), pin(2), pin(3), pin(4), pin(5), pin(6), pin(7), pin(8)
	Output: 1,2,3,4,5,6,7,8
	*/
	
	static void FifoTestCase1() {
		System.out.println("\n\nFIFO test case 1");
			Block blk1 = new Block("filename1", 1);
			Block blk2 = new Block("filename2", 2);
			Block blk3 = new Block("filename3", 3);
			Block blk4 = new Block("filename4", 4);
			Block blk5 = new Block("filename5", 5);
			Block blk6 = new Block("filename6", 6);
			Block blk7 = new Block("filename7", 7);
			Block blk8 = new Block("filename8", 8);
			Block blk9 = new Block("filename9", 9);
			Block blk10 = new Block("filename10", 10);
			Block blk11 = new Block("filename11", 11);
	
			BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
	
			try {
				basicBufferMgr.pin(blk1);
				basicBufferMgr.pin(blk2);
				basicBufferMgr.pin(blk3);
				basicBufferMgr.pin(blk4);
				basicBufferMgr.pin(blk5);
				basicBufferMgr.pin(blk6);
				basicBufferMgr.pin(blk7);
				basicBufferMgr.pin(blk8);
				System.out.println("BufferPool: " + basicBufferMgr.getBufferPool());
	
			} catch (BufferAbortException e) {
				e.printStackTrace();
			}
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk2));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk3));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk4));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk5));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk7));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk8));	
		}
		
			
	/*Test Case 2 Expected Output
	Execution: pin(1), pin(2), pin(3), pin(4), pin(5), pin(6), unpin(6), pin(7), pin(8), pin(10), unpin(1), pin(6)
	Output: 2,3,4,5,7,8,10,6
	*/
	
	static void FifoTestCase2() {
		System.out.println("\n\nFIFO test case 2");	
		Block blk1 = new Block("filename1", 1);
			Block blk2 = new Block("filename2", 2);
			Block blk3 = new Block("filename3", 3);
			Block blk4 = new Block("filename4", 4);
			Block blk5 = new Block("filename5", 5);
			Block blk6 = new Block("filename6", 6);
			Block blk7 = new Block("filename7", 7);
			Block blk8 = new Block("filename8", 8);
			Block blk9 = new Block("filename9", 9);
			Block blk10 = new Block("filename10", 10);
			Block blk11 = new Block("filename11", 11);
	
			BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
	
			try {
				basicBufferMgr.pin(blk1);
				basicBufferMgr.pin(blk2);
				basicBufferMgr.pin(blk3);
				basicBufferMgr.pin(blk4);		
				basicBufferMgr.pin(blk5);
				basicBufferMgr.pin(blk6);
				basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));		//Unpin block 6
				basicBufferMgr.pin(blk7);
				basicBufferMgr.pin(blk8);
				basicBufferMgr.pin(blk10);
				basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));		//Unpin block 1
				basicBufferMgr.pin(blk6);
				System.out.println("BufferPool: " + basicBufferMgr.getBufferPool());
	
			} catch (BufferAbortException e) {
				e.printStackTrace();
			}
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk2));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk3));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk4));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk5));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk7));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk8));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk10));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));	
		}
	
		
	/*Test Case 3 Expected Output
	Execution: pin(1), pin(2), pin(3), pin(4), pin(5), pin(6), unpin(6), pin(7), pin(8), pin(9), pin(10)
	Output: Exception will be raised as we have exceeded number of blocks we can pin.
	*/
	
	static void FifoTestCase3() {
		System.out.println("\n\nFIFO test case 3");
			Block blk1 = new Block("filename1", 1);
			Block blk2 = new Block("filename2", 2);
			Block blk3 = new Block("filename3", 3);
			Block blk4 = new Block("filename4", 4);
			Block blk5 = new Block("filename5", 5);
			Block blk6 = new Block("filename6", 6);
			Block blk7 = new Block("filename7", 7);
			Block blk8 = new Block("filename8", 8);
			Block blk9 = new Block("filename9", 9);
			Block blk10 = new Block("filename10", 10);
			Block blk11 = new Block("filename11", 11);
	
			BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
	
			try {
				basicBufferMgr.pin(blk1);
				basicBufferMgr.pin(blk2);
				basicBufferMgr.pin(blk3);
				basicBufferMgr.pin(blk4);		
				basicBufferMgr.pin(blk5);
				basicBufferMgr.pin(blk6);
				basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));		//Unpin block 6
				basicBufferMgr.pin(blk7);
				basicBufferMgr.pin(blk8);
				basicBufferMgr.pin(blk9);
				basicBufferMgr.pin(blk6);
				System.out.println("BufferPool: " + basicBufferMgr.getBufferPool());
	
			} catch (BufferAbortException e) {
				e.printStackTrace();
			}
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk2));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk3));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk4));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk5));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk7));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk8));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk9));	
		}
	
		
	/*Test Case 4 Expected Output
	Execution: pin(1), pin(2), pin(3), pin(4), pin(5), pin(6), pin(7), unpin(5), pin(8), pin(9), pin(1)
	Output: Pin count for block 1: 2
			1,2,3,4,9,6,7,8
	*/
	
	static void FifoTestCase4() {
		System.out.println("\n\nFIFO test case 4");
			Block blk1 = new Block("filename1", 1);
			Block blk2 = new Block("filename2", 2);
			Block blk3 = new Block("filename3", 3);
			Block blk4 = new Block("filename4", 4);
			Block blk5 = new Block("filename5", 5);
			Block blk6 = new Block("filename6", 6);
			Block blk7 = new Block("filename7", 7);
			Block blk8 = new Block("filename8", 8);
			Block blk9 = new Block("filename9", 9);
			Block blk10 = new Block("filename10", 10);
			Block blk11 = new Block("filename11", 11);
	
			BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
	
			try {
				basicBufferMgr.pin(blk1);
				basicBufferMgr.pin(blk2);
				basicBufferMgr.pin(blk3);
				basicBufferMgr.pin(blk4);		
				basicBufferMgr.pin(blk5);
				basicBufferMgr.pin(blk6);
				basicBufferMgr.pin(blk7);
				basicBufferMgr.unpin(basicBufferMgr.getMapping(blk5));		//Unpin block 5
				basicBufferMgr.pin(blk8);
				basicBufferMgr.pin(blk9);
				basicBufferMgr.pin(blk1);
				System.out.println("Pin count for block 1: "+basicBufferMgr.getMapping(blk1).pins);
				System.out.println("BufferPool: " + basicBufferMgr.getBufferPool());
			} catch (BufferAbortException e) {
				e.printStackTrace();
			}
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk2));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk3));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk4));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk7));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk8));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk9));	
		}
		
	
	/*Test Case 5 Expected Output
	Execution: pin(1), pin(2), pin(3), pin(4), pin(5), pin(6), pin(7), unpin(5), pin(8), pin(9), unpin(1), unpin(8), pin(1), pin(10)
	Output: 1,2,3,4,6,7,9,10
	*/
	
	static void FifoTestCase5() {
		System.out.println("\n\nFIFO test case 5");
			Block blk1 = new Block("filename1", 1);
			Block blk2 = new Block("filename2", 2);
			Block blk3 = new Block("filename3", 3);
			Block blk4 = new Block("filename4", 4);
			Block blk5 = new Block("filename5", 5);
			Block blk6 = new Block("filename6", 6);
			Block blk7 = new Block("filename7", 7);
			Block blk8 = new Block("filename8", 8);
			Block blk9 = new Block("filename9", 9);
			Block blk10 = new Block("filename10", 10);
			Block blk11 = new Block("filename11", 11);
	
			BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
	
			try {
				basicBufferMgr.pin(blk1);
				basicBufferMgr.pin(blk2);
				basicBufferMgr.pin(blk3);
				basicBufferMgr.pin(blk4);		
				basicBufferMgr.pin(blk5);
				basicBufferMgr.pin(blk6);
				basicBufferMgr.pin(blk7);
				basicBufferMgr.unpin(basicBufferMgr.getMapping(blk5));		//Unpin block 5
				basicBufferMgr.pin(blk8);
				basicBufferMgr.pin(blk9);
				basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));		//Unpin block 1
				basicBufferMgr.unpin(basicBufferMgr.getMapping(blk8));		//Unpin block 8
				basicBufferMgr.pin(blk1);
				basicBufferMgr.pin(blk10);
				System.out.println("BufferPool: " + basicBufferMgr.getBufferPool());
			} catch (BufferAbortException e) {
				e.printStackTrace();
			}
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk1));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk2));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk3));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk4));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk6));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk7));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk9));	
			basicBufferMgr.unpin(basicBufferMgr.getMapping(blk10));	
		}

}