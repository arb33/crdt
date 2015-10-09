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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

/**
 * Demo of the CRDT U-Set, supporting remove with garbage collection
 * @author Alastair R. Beresford
 *
 */
public class SetRemove {

	private int myClockIndex;
	private VectorClock[] allClocks;
	private Map<Integer, Set<VectorClock>> s = new HashMap<Integer, Set<VectorClock>>();
	private Map<Integer, Set<VectorClock>> r = new HashMap<Integer, Set<VectorClock>>();

	private SetRemove() {}

	public SetRemove(VectorClock myClock, int myClockIndex) {

		this.myClockIndex = myClockIndex;
		allClocks = new VectorClock[myClock.size()];
		for(int i = 0; i < allClocks.length; i++) {
			allClocks[i] = (i == myClockIndex) ? myClock.getDeepCopy() : new VectorClock(myClock.size());
		}
	}

	public SetRemove getDeepCopy() {

		SetRemove clone = new SetRemove();

		clone.myClockIndex = myClockIndex;
		clone.allClocks = new VectorClock[allClocks.length];
		for(int i = 0; i < allClocks.length; i++) {
			clone.allClocks[i] = allClocks[i].getDeepCopy();
		}
		for(Entry<Integer, Set<VectorClock>> entry: s.entrySet()) {
			Set<VectorClock> cloneTimestamps = new HashSet<VectorClock>();
			for(VectorClock clock: entry.getValue()) {
				cloneTimestamps.add(clock.getDeepCopy());
			}
			clone.s.put(entry.getKey(), cloneTimestamps);
		}
		for(Entry<Integer, Set<VectorClock>> entry: r.entrySet()) {
			Set<VectorClock> cloneTimestamps = new HashSet<VectorClock>();
			for(VectorClock clock: entry.getValue()) {
				cloneTimestamps.add(clock.getDeepCopy());
			}
			clone.r.put(entry.getKey(), cloneTimestamps);
		}

		return clone;
	}

	private void add(Map<Integer, Set<VectorClock>> set, int value) {

		Set<VectorClock> timestamps = set.get(value);
		VectorClock currentLocalTime = allClocks[myClockIndex].getDeepCopy();
		if (timestamps == null) {
			timestamps = new HashSet<VectorClock>();
			timestamps.add(currentLocalTime);
			set.put(value, timestamps);
		} else {
			timestamps.add(currentLocalTime);
		}
	}

	public void add(int value) {

		allClocks[myClockIndex].inc(myClockIndex);		
		add(s, value);
	}

	public boolean remove(int value) {

		if (s.containsKey(value)) {

			allClocks[myClockIndex].inc(myClockIndex);
			
			Set<VectorClock> cloneOfSClocks = new HashSet<VectorClock>();
			for(VectorClock clock : s.get(value)) {
				cloneOfSClocks.add(clock.getDeepCopy());
			}

			Set<VectorClock> rClocks = r.get(value);
			if (rClocks == null) {
				r.put(value, cloneOfSClocks);
			} else {
				cloneOfSClocks.addAll(rClocks);
				r.put(value,  cloneOfSClocks);
			}
			
			return true;
		}
		return false;
	}

	private Map<Integer, Set<VectorClock>> mergeMapIntegerSetVectorClocks(Map<Integer, Set<VectorClock>> map1, Map<Integer, Set<VectorClock>> map2) {

		Map<Integer, Set<VectorClock>> map = new HashMap<Integer, Set<VectorClock>>();

		for(Entry<Integer, Set<VectorClock>> entry: map1.entrySet()) {

			Integer key = entry.getKey();
			Set<VectorClock> timestamps = entry.getValue();
			Set<VectorClock> otherTimestamps = map2.get(key);
			if (otherTimestamps != null) {
				timestamps.addAll(otherTimestamps); //TODO(arb33): what happens if otherTimestamps is null?
				map2.remove(key);
			}
			map.put(key, timestamps);
		}
		map.putAll(map2);
		return map;
	}

	public void mergeIn(SetRemove senderSet) {

		//On message receipt, increment our own value in our local clock by one
		allClocks[myClockIndex].inc(myClockIndex);

		//Update our cache of remote clocks to reflect the values seen by the sender
		for(int i = 0; i < allClocks.length; i++) {
			allClocks[i].mergeIn(senderSet.allClocks[i]);
		}

		//Update our clock to record that we have seen all the events seen by the sender
		allClocks[myClockIndex].mergeIn(senderSet.allClocks[senderSet.myClockIndex]);

		//Merge the actual values of sets S and R
		s = mergeMapIntegerSetVectorClocks(s, senderSet.s);
		r = mergeMapIntegerSetVectorClocks(r, senderSet.r);

		//Each entry, e, in R has an associated set of vector clocks called vr(e)
		//Each entry, e, in S has an associated set of vector clocks called vs(e)
		//We need to remove all vs(e) which occurred EARLIER than the any vr(e).
		//If, after this operation, vs(e) is empty, then we remove e from S.
		//We then remove all vr(e) which are EARLIER than the local copies of *all* remote clocks (and our own)
		//If, after removing elements from vr(e), vr(e) is empty we can remove e from R.

		Map<Integer, Set<VectorClock>> newS = new HashMap<Integer, Set<VectorClock>>();		
		for(Entry<Integer, Set<VectorClock>> entry: s.entrySet()) {

			Integer key = entry.getKey();
			Set<VectorClock> rTimestamps = r.get(key);
			Set<VectorClock> sTimestamps = entry.getValue();
			Set<VectorClock> newSTimestamps = new HashSet<VectorClock>();

			if (rTimestamps != null) {				
				for(VectorClock sClock : sTimestamps) {
					for(VectorClock rClock : rTimestamps) {
						if (sClock.compare(rClock) != VectorClock.Comparison.EARLIER) {
							newSTimestamps.add(sClock);
						}
					}
				}
				if (newSTimestamps.size() > 0) {
					newS.put(key, newSTimestamps);
				}
			} else {
				newS.put(key, sTimestamps);
			}
		}
		s = newS;
		Map<Integer, Set<VectorClock>> newR = new HashMap<Integer, Set<VectorClock>>();
		for(Entry<Integer, Set<VectorClock>> entry: r.entrySet()) {

			Set<VectorClock> timestamps = entry.getValue();

			Set<VectorClock> newTimestamps = new HashSet<VectorClock>();
			for(VectorClock timestamp : timestamps) {
				boolean timestampEarlierThanAllClocks = true;
				for(int i = 0; i < allClocks.length; i++) {
					if(timestamp.compare(allClocks[i]) != VectorClock.Comparison.EARLIER) {
						timestampEarlierThanAllClocks = false;
						break;
					}
				}
				if (!timestampEarlierThanAllClocks) {
					newTimestamps.add(timestamp);
				}
			}
			if (newTimestamps.size() > 0)
				newR.put(entry.getKey(), newTimestamps);
		}
		r = newR;
	}

	public boolean contains(int i) {
		
		Set<VectorClock> clocksInS = s.get(i);
		if (clocksInS == null) {
			return false; //i is not in S.
		}

		Set<VectorClock> clocksInR = r.get(i);
		if (clocksInR == null) {
			return true; //i is in S and not in R.
		}

		for(VectorClock clock : clocksInS) {
			if (!clocksInR.contains(clock)) {
				return true; //there is at least one version of i in S which is not scheduled for deletion
			}
		}
		return true; //All versions of i in S are marked for deletion in R		
	}

	public Integer[] values() {
		List<Integer> values = new ArrayList<Integer>();
		
		for(Integer v: s.keySet()) { //TODO(arb33) Could probably do this more efficiently...
			if (contains(v)) {
				values.add(v);
			}
		}
		
		Integer[] array = new Integer[s.size()];
		return values.toArray(array);
	}

	@Override
	public String toString() {
		String result = "s: " + s.toString() + " r: " + r.toString() + " clock: [";
		for(VectorClock clock: allClocks) {
			result += clock;
		}
		return result +  "]";
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof SetRemove) {
			SetRemove other = (SetRemove) obj;
			return s.keySet().equals(other.s.keySet());
		}		
		return super.equals(obj);
	}
	/**
	 * Run a period of random additions, removals and merges, then one comms loop through all and check for consistency.
	 */
	private static boolean testSet(int seed, int numberEvents, int numberDevices, int maxNumberInSet, boolean debug) {

		Random r = (seed == 0) ? new Random() : new Random(seed);
		SetRemove[] devices = new SetRemove[numberDevices];
		for(int i = 0; i < devices.length; i++) {
			devices[i] = new SetRemove(new VectorClock(devices.length), i);
			if (debug) System.out.println(devices[i]);
		}
		if (debug) System.out.println();

		//pick a device and an event at random and perform it.
		for(int i = 0; i < numberEvents; i++) {
			int d = r.nextInt(devices.length);
			int opcode = r.nextInt(3);
			switch(opcode) {
			case 0:
				int number = r.nextInt(maxNumberInSet);
				if(debug) {
					System.out.println("Add " + number + " to node " + d);
				}
				devices[d].add(number);
				break;
			case 1:
				Integer[] values = devices[d].values();
				if (values.length > 0) {
					number = r.nextInt(values.length);
					if(debug) {
						System.out.println("Attempting to remove " + values[number] + " from node " + d);
					}
					devices[d].remove(values[number]);
				} else {
					if(debug) {
						System.out.println("Remove failed since " + d + " is empty");
					}
				}
				break;
			case 2:
				int remote = r.nextInt(devices.length);
				if(debug) {
					System.out.println("Merge with d = " + d + " and remote = " + remote);
				}
				devices[d].mergeIn(devices[remote].getDeepCopy());
			}

			if (debug) {
				for(SetRemove device : devices) {
					System.out.println(device);
				}
				System.out.println();
			}
		}


		//Need to copy to/from device zero twice in order to ensure all local copies of node clocks are greater than
		//any message. This then means all values can be flushed from R (so R = {}) and consequently all nodes agree on the contents of S.
		for(int j = 0; j < 2; j++) {
			if (debug) {
				System.out.println("Merging from device i to  device zero");
			}
			//Use first device as a hub to merge things to/from to ensure consistent state
			for(int i = 1; i < devices.length; i++) {
				devices[0].mergeIn(devices[i].getDeepCopy());
				if (debug) {
					System.out.println("Merge with d = " + 0 + " and remote = " + i);
					for(SetRemove device : devices) {
						System.out.println(device);
					}
					System.out.println();
				}
			}
			if (debug) {
				System.out.println("Merging from device 0 to device i");
			}
			for(int i = 1; i < devices.length; i++) {
				devices[i].mergeIn(devices[0].getDeepCopy());
				if (debug) {
					System.out.println("Merge with d = " + 0 + " and remote = " + i);
					for(SetRemove device : devices) {
						System.out.println(device);
					}
					System.out.println();
				}
			}
		}

		for(int i = 1; i < devices.length; i++) {
			if (!devices[0].equals(devices[i])) {
				System.out.println("Failure for seed = " + seed + " events = " + numberEvents);
				for(SetRemove device : devices) {
					System.out.println(device);
				}
				return false;
			}
		}

		if(debug) {
			System.out.println("Success. All copies of S are identical.");
		}
		return true;
	}

	public static void main(String[] args) {
		for(int numberEvents = 0; numberEvents < 30; numberEvents++) {
			for(int numberDevices = 2; numberDevices < 6; numberDevices++) {
				for(int maxNumber = 5; maxNumber < 30; maxNumber+=5) {
					for(int seed = 1; seed < 5000; seed++) {
						if(!testSet(seed, numberEvents, numberDevices, maxNumber, false)) {
							System.out.println("**Turning on debugging...\n\n");
							testSet(seed, numberEvents, numberDevices, maxNumber, true);
							return;
						}
					}
				}		
			}
		}
	}
}
