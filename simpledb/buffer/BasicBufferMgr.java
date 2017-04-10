package simpledb.buffer;

import java.util.LinkedHashMap;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	// private Buffer[] bufferPoolMap;
	public LinkedHashMap<Block, Buffer> bufferPoolMap;
	private int numAvailable;
	private int bufferSize;

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferPoolMap = new LinkedHashMap<Block, Buffer>(numbuffs);
		numAvailable = numbuffs;
		bufferSize = numbuffs;
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferPoolMap.values())
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			//System.out.println("Unpinned Buffer: " + buff);
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
			bufferPoolMap.put(blk, buff);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {
		if (bufferPoolMap.containsKey(blk)) {
			return bufferPoolMap.get(blk);
		} else {
			return null;
		}
	}

	private Buffer chooseUnpinnedBuffer() {
		Buffer buff = new Buffer();
		if (bufferPoolMap.size() < bufferSize) {
			// System.out.println(bufferPoolMap.size());
			return buff;
		}
		for (Block blk : bufferPoolMap.keySet()) {
			buff = bufferPoolMap.get(blk);
			if (!buff.isPinned()) {
				bufferPoolMap.remove(blk);
				return buff;
			}
		}
		return null;
	}

	boolean containsMapping(Block blk) {
		return bufferPoolMap.containsKey(blk);
	}

	Buffer getMapping(Block blk) {
		return bufferPoolMap.get(blk);
	}

	/**
	 * Returns the map of block-buffer current residing in buffer pool.
	 */
	public String getBufferPool() {
		return String.valueOf(bufferPoolMap);
	}
}