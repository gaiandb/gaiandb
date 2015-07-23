/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A concurrent Queue that is optimized for the conditions:
 * 	- There can be multiple producers, but only 1 consumer. The consumer only has to wait when there is no data.
 *  - The only methods implemented are: offer(Object), poll(), poll(time, unit), take(), size() (which is expensive)
 *  - For method poll(time, unit), unit is ignored and assumed to be MILLISECONDS
 *  - The producers do not attempt to offer an element to the Queue when it has reached its maximum capacity.
 * 
 * @author DavidVyvyan
 */

public class ConcurrentWriteQueue<E> implements BlockingQueue<E> {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";	

	CircularBufferNode in, out;
	
	Object consumerLock = new Object(), producerLock = new Object();
	
	private class CircularBufferNode {
		E elmt;
		CircularBufferNode tail;
	}
	
	public ConcurrentWriteQueue( int capacity ) {
		
		if ( 0 == capacity ) return;
		CircularBufferNode first = new CircularBufferNode();
		CircularBufferNode next = first;
		for (int i=0; i<capacity-1; i++, next=next.tail)
			next.tail = new CircularBufferNode();
		
		next.tail = first;
		
		in = first;
		out = in;
	}
	
	public boolean offer(E o) {
		
		boolean wasEmpty;
		
		synchronized( producerLock ) {			
			wasEmpty = in == out;			
			in.elmt = o;
			in = in.tail;
		}
		
		if ( wasEmpty ) synchronized( consumerLock ) { consumerLock.notify(); } // notify potentially waiting thread
		
		return true;
/*		
		boolean wasEmpty = isEmpty();
		if ( super.offer(o) ) {
			if ( wasEmpty ) synchronized(this) { notify(); }
			return true;
		}
		return false;
*/
	}
	
	public E poll() {
		if ( out == in ) return null;
		E o = out.elmt;
		out = out.tail;
		return o;
	}

	public E poll(long timeout, TimeUnit unit) throws InterruptedException {

		if ( out == in ) synchronized( consumerLock ) { consumerLock.wait( timeout ); }
		E o = out.elmt;
		out = out.tail;
		return o;
	}

	public E take() throws InterruptedException {
		return poll(0, TimeUnit.MILLISECONDS);
	}
	
	public int size() {
		CircularBufferNode snapIn, snapOut;
		synchronized (this) {
			snapOut = out;
			snapIn = in;
		}
		int size = 0;
		for ( CircularBufferNode n = snapOut; n != snapIn ; n = n.tail ) size++;
		
		return size;
	}

	public int drainTo(Collection<? super E> c) {
		return 0;
	}

	public int drainTo(Collection<? super E> c, int maxElements) {
		return 0;
	}


	public void put(Object o) throws InterruptedException {
		
	}

	public int remainingCapacity() {
		return 0;
	}
	
	public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException {
		return false;
	}
	
	public boolean add(Object o) {
		return false;
	}

	public E element() {
		return null;
	}

	public E peek() {
		return null;
	}

	public E remove() {
		return null;
	}

	public boolean addAll(Collection<? extends E> c) {
		return false;
	}

	public void clear() {
	}

	public boolean contains(Object o) {
		return false;
	}

	public boolean containsAll(Collection<?> c) {
		return false;
	}

	public boolean isEmpty() {
		return false;
	}

	public Iterator<E> iterator() {
		return null;
	}

	public boolean remove(Object o) {
		return false;
	}

	public boolean removeAll(Collection<?> c) {
		return false;
	}

	public boolean retainAll(Collection<?> c) {
		return false;
	}

	public Object[] toArray() {
		return null;
	}

	public <T> T[] toArray(T[] a) {
		return null;
	}
}
