/** A sequential implementation of the Sieve of Eratosthenes */

import ox.cads.util.ThreadUtil
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray

object ConcurrentSieve{

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
   
    var printed = new AtomicInteger(0)

    // The function that each thread runs, taking it's own id as a parameter
    def comp(me: Int) {
      // Test integers until all integers under N have been tested
      while(nextSlot.get<N) {
        // Get the next integer to test for primality
        var candidate = next.getAndIncrement
        // Convert the candidate to a long to prevent overflow when squaring
        var l_candidate = candidate.toLong
        // Update the current array with this thread's current prime candidate
        current.set(me, candidate)

        // Wait until primes below sqrt(candidate) being tested are released
        var thread = 0
        while (thread < T) {
          while (squareLong(current.get(thread)) < l_candidate) {}
          thread += 1
        }

        // Test if candidate is prime
        // invariant: candidate is coprime with primes[0..i) && p = primes(i)
        var i = 0; var p = primes.get(i)
        println(p)
        while(p*p<=candidate && candidate%p != 0){
          i += 1;
          p = primes.get(i)
        }
        println("Testing " + candidate.toString)
        if(p*p>candidate){ // candidate is prime
          println("Found prime " + candidate.toString)
          // Get the next available slot
          // A local copy of the nextSlot atomic integer
          var candidateSlot = nextSlot.get
          // Represents the position of p in the sorted array
          var pos = candidateSlot
          // Represents the maximum value of the array
          var max_prime = scala.math.max(candidate, primes.get(candidateSlot - 1))
          do {
            candidateSlot = nextSlot.get
            // Test where the prime candidate should go in the sorted array
            pos = candidateSlot
            while (primes.get(pos - 1) > candidate) {
              // The prime is smaller than the prime at position (pos - 1), so
              // p appears further back in the array
              pos -= 1
            }
          } while (!primes.compareAndSet(candidateSlot, 0, max_prime))
          // Copy primes[pos, candidateSlot - 1) up the array by 1 position
          for (index <- candidateSlot to pos + 1 by -1) {
             primes.set(index, primes.get(index - 1))
          }
          // Put the candidate in the primes array at the correct place
          primes.set(pos, candidate)
          // Increment the nextSlot variable
          nextSlot.getAndIncrement
        }
      }
    }

    // Run T threads of comp concurrently, giving each a uid between 0 and T
    ThreadUtil.runIndexedSystem(T, comp)    
              println("Primes: ")
              for (n <- 0 to (N + T - 2)) print(primes.get(n).toString + ", ")
              println(".")

    println(primes.get(N-1))
    println("Time taken: "+(java.lang.System.currentTimeMillis()-t0))
  }
}

