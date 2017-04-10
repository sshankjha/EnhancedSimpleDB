package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import java.util.Iterator;
import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

/**
 * A class that provides the ability to read records from the log in reverse
 * order. Unlike the similar class {@link simpledb.log.LogIterator LogIterator},
 * this class understands the meaning of the log records.
 * 
 * @author Edward Sciore
 */
public class LogRecordIterator implements Iterator<LogRecord> {
	private Iterator<BasicLogRecord> iter = SimpleDB.logMgr().iterator();
	private Iterator<BasicLogRecord> iterFwd = SimpleDB.logMgr().iteratorForward();
	private Iterator<BasicLogRecord> iterFwdFromStart = SimpleDB.logMgr().iteratorForwardFromStart();

	public boolean hasNext() {
		return iter.hasNext();
	}

	public boolean hasNextFwd() {
		return iterFwd.hasNext();
	}

	public boolean hasNextFwdFromStart() {
		return iterFwdFromStart.hasNext();
	}

	/**
	 * Constructs a log record from the values in the current basic log record.
	 * The method first reads an integer, which denotes the type of the log
	 * record. Based on that type, the method calls the appropriate LogRecord
	 * constructor to read the remaining values.
	 * 
	 * @return the next log record, or null if no more records
	 */
	public LogRecord next() {
		BasicLogRecord rec = iter.next();
		return getLogRecord(rec);
	}

	public LogRecord nextFwd() {
		BasicLogRecord rec = iterFwd.next();
		return getLogRecord(rec);
	}

	public LogRecord nextFwdFromStart() {
		BasicLogRecord rec = iterFwdFromStart.next();
		return getLogRecord(rec);
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	private LogRecord getLogRecord(BasicLogRecord rec) {
		int op = rec.nextInt();
		switch (op) {
		case CHECKPOINT:
			return new CheckpointRecord(rec);
		case START:
			return new StartRecord(rec);
		case COMMIT:
			return new CommitRecord(rec);
		case ROLLBACK:
			return new RollbackRecord(rec);
		case SETINT:
			return new SetIntRecord(rec);
		case SETSTRING:
			return new SetStringRecord(rec);
		default:
			return null;
		}
	}
}
