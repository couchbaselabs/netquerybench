using System;
using System.Collections.Generic;

namespace netquerybench.workload
{
    class DiscreteGenerator
    {
        
        class Pair
        {
            public String _value;
            public double _weight;

            public Pair(double weight, string value)
            {
                this._weight = weight;
                this._value = value;
            }
        }

        private List<Pair> valPairs = new List<Pair>();
        private Random randomGenerator = new Random();

        public string NextString()
        {
            double sum = 0;
            foreach (Pair p in valPairs)
            {
                sum += p._weight;
            }
            double val = randomGenerator.NextDouble();
            foreach (var p in valPairs)
            {
                if (val < p._weight / sum)
                {
                    return p._value;
                }

                val -= p._weight / sum;
            }
            return null;
        }

        public void AddValue(double weight, String value)
        {
            Pair pair = new Pair(weight, value);
            valPairs.Add(pair);
        }

    }
}
