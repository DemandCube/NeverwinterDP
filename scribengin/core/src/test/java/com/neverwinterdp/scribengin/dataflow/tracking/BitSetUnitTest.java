package com.neverwinterdp.scribengin.dataflow.tracking;

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

import org.junit.Test;

import com.neverwinterdp.util.io.IOUtil;

public class BitSetUnitTest {
  @Test
  public void testBitSet() throws IOException {
    int NUM_OF_BITS = 32 * 1024;
    BitSet bitSet = new BitSet(NUM_OF_BITS);
    System.out.println("before compressed: " + IOUtil.compress(bitSet.toByteArray()).length);
    Random rand = new Random();
    int idx = 0;
    while(idx < NUM_OF_BITS/2) {
      bitSet.set(idx, true);
      while(rand.nextDouble() < 0.25) {
        idx += rand.nextInt(5);
      }
    }
    System.out.println("half compressed: " + IOUtil.compress(bitSet.toByteArray()).length);
    
    while(idx < NUM_OF_BITS) {
      bitSet.set(idx, true);
      while(rand.nextDouble() < 0.25) {
        idx += rand.nextInt(5);
      }
    }
    System.out.println("after compressed: " + IOUtil.compress(bitSet.toByteArray()).length);
  }
}
