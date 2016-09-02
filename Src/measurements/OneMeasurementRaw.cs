using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
namespace netquerybench.measurements
{
   
    public class OneMeasurementRaw : OneMeasurement
    {
        public double min = 0;
        public double max = 0;
        public double avg = 0;
        public int count = 0;
        public double total = 0;
        object Lock = new object();
        SortedSet<double> latencies = new SortedSet<double>();

        public override void Measure(double latency)
        {
            lock (Lock)
            {
                if (min == 0 || latency < min)
                {
                    min = latency;
                }
                if (max == 0 || latency > max)
                {
                    max = latency;
                }
                total += latency;
                count += 1;
                avg = total/count;
                latencies.Add(latency);
            }
        }

        public override int GetCount()
        {
            return count;
        }

        public override string GetSummary()
        {

            int ninetyfiveIndex = (latencies.Count * 90)/100;
            double ninetyfiveLatency = latencies.ElementAt(ninetyfiveIndex);
            StringBuilder sb = new StringBuilder();
            sb.Append("Operations: " +  count);
            sb.Append(Environment.NewLine);
            sb.Append("AverageLatency(ms): " + avg);
            sb.Append(Environment.NewLine);
            sb.Append("Min Latency(ms): " + min);
            sb.Append(Environment.NewLine);
            sb.Append("Max Latency(ms): " + max);
            sb.Append(Environment.NewLine);
            sb.Append("95th percentile latency(ms): " + ninetyfiveLatency);
            sb.Append(Environment.NewLine);
            return sb.ToString();
        }
    }
}
