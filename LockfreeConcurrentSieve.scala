/** A sequential implementation of the Sieve of Eratosthenes */

import ox.cads.util.ThreadUtil
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray

object LockfreeConcurrentSieve{

  def squareLong(n: Int): Long = {
    val l_n = n.toLong
    l_n * l_n
  }

  def main(args: Array[String]) = {
    assert(args.length == 2, "Must have two arguments")
    val t0 = java.lang.System.currentTimeMillis()

    val N = args(0).toInt // number of primes required
    val T = args(1).toInt // Number of threads to use
    val primes = new AtomicIntegerArray(N + T - 1) // will hold the primes
    // Holds the prime candidate each thread is currently testing
    val current = new AtomicIntegerArray(T)
    primes.set(0, 2)
    var nextSlot = new AtomicInteger(1) // next free slot in primes
    var next = new AtomicInteger(3) // next candidate prime to consider

    // The function that each thread runs, taking it's own id as a parameter
    def comp(me: Int) {
      var lastIndex = 0
      // Test integers until all integers under N have been tested
      while(nextSlot.get<N) {
        // Get the next integer to test for primality
        var candidate = 0
        do {
          candidate = next.get
          // Update the current array with this thread's current prime candidate
          current.set(me, candidate)
        } while (!next.compareAndSet(candidate, candidate + 1))
        // Convert the candidate to a long to prevent overflow when squaring
        var l_candidate = candidate.toLong

        // Wait until primes below sqrt(candidate) being tested are released
        var thread = 0
        while (thread < T) {
          while (squareLong(current.get(thread)) <= l_candidate) {}
          thread += 1
        }

        // Test if candidate is prime
        // invariant: candidate is coprime with primes[0..i) && p = primes(i)
        var i = 0; var p = primes.get(i)
        while(p != 0 && p*p<=candidate && candidate%p != 0){
          i += 1; 
          p = primes.get(i)
        }
        if(p == 0 || p*p>candidate){ // candidate is prime
          var primeToInsert = candidate
          var ii = lastIndex
          while (ii <= nextSlot.get) {
            val primeAtIndex = primes.get(ii)
            if (primeAtIndex > primeToInsert || primeAtIndex == 0) {
              if (primes.compareAndSet(ii, primeAtIndex, primeToInsert)) {
                if (primeToInsert == candidate) lastIndex = ii
                primeToInsert = primeAtIndex
              }
            } else {
              ii += 1
            }
          }
          nextSlot.getAndIncrement
        }
      }
    }

    // Run T threads of comp concurrently, giving each a uid between 0 and T
    ThreadUtil.runIndexedSystem(T, comp)

    println(primes.get(N-1))
    println("Time taken: "+(java.lang.System.currentTimeMillis()-t0))
  }
}

