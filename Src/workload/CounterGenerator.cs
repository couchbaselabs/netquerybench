using System;
using System.Threading;

namespace netquerybench.workload
{
    class CounterGenerator : IntegerGenerator
    {
        private int counter = 0;


        public CounterGenerator(int countStart)
        {
            counter = countStart;
        }

        public int NextInt()
        {
            Interlocked.Increment(ref counter);
            return counter;
        }


    }
}
