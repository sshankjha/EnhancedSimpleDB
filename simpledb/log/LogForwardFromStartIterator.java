package simpledb.log;

import static simpledb.file.Page.INT_SIZE;

import java.util.Iterator;
import java.util.Stack;

import simpledb.file.Block;
import simpledb.file.Page;

/**
 * A class that provides the ability to move through the records of the log file
 * in forward order from starting of log.
 */
class LogForwardFromStartIterator implements Iterator<BasicLogRecord> {

	private Page pg = new Page();
	private Block blk;
	private int currentrec;
	private int finalStackFromStartSize;
	private Stack<Integer> forwardStackFromStart = new Stack<Integer>();
	private boolean isStackReady;

	LogForwardFromStartIterator(Block blk) {
		this.blk = blk;
		pg.read(blk);
		currentrec = pg.getInt(LogMgr.LAST_POS);
		isStackReady=false;
	}

	private void makeForwardStackFromStart() {
		int tmpAddress;
		boolean continueLoop = true;
		while (continueLoop && hasPrevious()) {
			tmpAddress = previousAddress();
			forwardStackFromStart.push(tmpAddress);
			// System.out.println("Address being pushed: " + tmpAddress);
		}
		System.out.println("Size of stack: " + forwardStackFromStart.size());
		isStackReady=true;
		finalStackFromStartSize = forwardStackFromStart.size();
	
	}

	public boolean hasNext() {
		if (!isStackReady) {
			makeForwardStackFromStart();
		}
		return !forwardStackFromStart.isEmpty();
	}

	public BasicLogRecord next() {
		if (!isStackReady) {
			makeForwardStackFromStart();
		}
		int addressToPop = forwardStackFromStart.pop();
		if (addressToPop == 0 && finalStackFromStartSize != forwardStackFromStart.size()) {
			blk = new Block(blk.fileName(), blk.number() + 1);
			pg.read(blk);
		}
		// System.out.println("Address being popped: " + addressToPop);
		return new BasicLogRecord(pg, addressToPop + INT_SIZE);
	}

	private  boolean hasPrevious() {
		return currentrec > 0 || blk.number() > 0;
	}

	private int previousAddress() {
		if (currentrec == 0)
			moveToPreviousBlock();
		currentrec = pg.getInt(currentrec);
		return currentrec;
	}

	private void moveToPreviousBlock() {
		// System.out.println("Going to previous block");
		blk = new Block(blk.fileName(), blk.number() - 1);
		pg.read(blk);
		currentrec = pg.getInt(LogMgr.LAST_POS);
	}
}
