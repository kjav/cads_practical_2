/** A sequential implementation of the Sieve of Eratosthenes */

object ConcurrentSieve{

  def main(args: Array[String]) = {
    assert(args.length == 1, "must have one argument")
    val t0 = java.lang.System.currentTimeMillis()

    val N = args(0).toInt // number of primes required
    val primes = new Array[Int](N) // will hold the primes
    primes(0) = 2
    var nextSlot = 1 // next free slot in primes
    var next = 3 // next candidate prime to consider
    
    while(nextSlot<N){
      // Test if next is prime; 
      // invariant: next is coprime with primes[0..i) && p = primes(i)
      var i = 0; var p = primes(i)
      while(p*p<=next && next%p != 0){
        i += 1;
        p = primes(i)
      }
      if(p*p>next){ // next is prime
	      primes(nextSlot) = next; nextSlot += 1
      }
      next += 1
    }

    println(primes(N-1))
    println("Time taken: "+(java.lang.System.currentTimeMillis()-t0))
  }
}

