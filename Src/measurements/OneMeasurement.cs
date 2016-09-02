using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Annotations;

namespace netquerybench.measurements
{
    public abstract class OneMeasurement
    {
        public abstract void Measure(double latency);
        public abstract string GetSummary();
        public abstract int GetCount();
    }
}
