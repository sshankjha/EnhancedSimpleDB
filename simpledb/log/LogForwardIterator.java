package simpledb.log;

import static simpledb.file.Page.INT_SIZE;

import java.util.Iterator;
import java.util.Stack;

import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.tx.recovery.LogRecord;

/**
 * A class that provides the ability to move through the records of the log file
 * in forward order from last checkpoint.
 */
class LogForwardIterator implements Iterator<BasicLogRecord> {

	private Page pg = new Page();
	private Block blk;
	private int currentrec;
	private int finalStackSize;
	private Stack<Integer> forwardStack = new Stack<Integer>();
	private boolean isStackReady;

	LogForwardIterator(Block blk) {
		this.blk = blk;
		pg.read(blk);
		currentrec = pg.getInt(LogMgr.LAST_POS);
		isStackReady=false;
	}

	private void makeForwardStack() {
		int tmpAddress;
		boolean continueLoop = true;
		while (continueLoop && hasPrevious()) {
			tmpAddress = previousAddress();
			BasicLogRecord blr = new BasicLogRecord(pg, tmpAddress + INT_SIZE);
			if (blr.nextInt() == LogRecord.CHECKPOINT) {
				continueLoop = false;
				break;
			} else {
				forwardStack.push(tmpAddress);
				// System.out.println("Address being pushed: " + tmpAddress);
			}
		}
		isStackReady=true;
		finalStackSize = forwardStack.size();
		//System.out.println("Size of stack: " + forwardStack.size());
	}

	public boolean hasNext() {
		if (!isStackReady) {
			makeForwardStack();
		}
		return !forwardStack.isEmpty();
	}

	public BasicLogRecord next() {
		if (!isStackReady) {
			makeForwardStack();
		}
		int addressToPop = forwardStack.pop();
		if (addressToPop == 0 && finalStackSize != forwardStack.size()) {
			blk = new Block(blk.fileName(), blk.number() + 1);
			pg.read(blk);
		}
		// System.out.println("Address being popped: " + addressToPop);
		return new BasicLogRecord(pg, addressToPop + INT_SIZE);
	}

	private boolean hasPrevious() {
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
