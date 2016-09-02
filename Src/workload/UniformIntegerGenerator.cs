using System;

namespace netquerybench.workload
{
    public class UniformIntegerGenerator : IntegerGenerator
    {
        private int _lb;
        private int _ub;
        private Random r;

        public UniformIntegerGenerator(int lb, int ub)
        {
            _lb = lb;
            _ub = ub;
            r = new Random();
        }

        public int NextInt()
        {
            int ret = r.Next(_lb, _ub);
            return ret;
        }
    }
}
