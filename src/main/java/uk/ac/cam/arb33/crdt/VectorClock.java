/**
 * Copyright 2015 Alastair R. Beresford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.cam.arb33.crdt;

public class VectorClock {
	
	private int[] vector;		
	public enum Comparison {LATER, EQUAL, EARLIER, SIMULTANEOUS};
	
	private VectorClock() {}
	
	public VectorClock(int size) {
		vector = new int[size];
	}
	
	public VectorClock(int[] clock) {
		vector = clock.clone();
	}
	
	public VectorClock getDeepCopy() {
		VectorClock clone = new VectorClock();
		clone.vector = vector.clone();
		return clone;
	}

	public void inc(int index) {
		vector[index]++;
	}
	
	public int size() {
		return vector.length;
	}
	
	public int get(int index) {
		//TODO(arb33): check for out-of-bounds issue
		return vector[index];
	}
	
	public VectorClock.Comparison compare(VectorClock v) {
		boolean greater = true;
		boolean smaller = true;
		boolean equal = true;
		
		for(int i = 0; i < vector.length; i++) {
			if(vector[i] > v.vector[i]) {
				smaller = false;
				equal = false;
			}
			if(vector[i] < v.vector[i]) {
				greater = false;
				equal = false;
			}
		}

		if (equal)
			return Comparison.EQUAL;
		
		if (greater && !smaller)
			return Comparison.LATER;
		
		if (smaller && !greater)
			return Comparison.EARLIER;
		
		return Comparison.SIMULTANEOUS;
	}
	
	//Merge clock data from other into this clock
	public VectorClock mergeIn(VectorClock other) {
		//TODO(arb33): Better cope (raise Exception?) with the fact that clocks might be different lengths.
		for(int i = 0; i < other.vector.length && i < vector.length; i++) {
			vector[i] = (vector[i] < other.vector[i]) ? other.vector[i] : vector[i];
		}
		return this;
	}	
	
	@Override
	public String toString() {
		String result = "[";
		for(int i: vector) {
			result += i + " ";
		}
		return result + "]";
	}
	
	@Override
	public int hashCode() {
		//TODO(arb33): Think more carefully about this; potentially a little brittle at the moment.
		int hash = 0;
		for(int i = 0; i < vector.length; i++) {
			hash += vector[i] << (i % 31);
		}
		return hash;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof VectorClock) {
			VectorClock other = (VectorClock) obj;
			if (vector.length != other.vector.length) {
				return false;
			}
			for(int i = 0; i < vector.length; i++) {
				if (vector[i] != other.vector[i]) {
					return false;
				}
			}
			return true;
 		}
		return super.equals(obj);
	}
	
	public static void main(String[] args) {
		VectorClock test = new VectorClock(new int[]{1,0,1,1});

		VectorClock earlier = new VectorClock(new int[]{1,0,0,1});
		VectorClock simultaneous = new VectorClock(new int[]{1,1,0,1});
		VectorClock equal = new VectorClock(new int[]{1,0,1,1});
		VectorClock later = new VectorClock(new int[]{1,0,2,1});
		
		assert(test.compare(earlier) == Comparison.EARLIER);
		assert(test.compare(later) == Comparison.LATER);
		assert(test.compare(equal) == Comparison.EQUAL);
		assert(test.compare(simultaneous) == Comparison.SIMULTANEOUS);
	}
}
