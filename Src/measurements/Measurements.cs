using System;
using System.Collections.Concurrent;
using netquerybench.client;


namespace netquerybench.measurements
{
    public class Measurements
    {
        static Measurements singleton = null;
        private ConcurrentDictionary<string, OneMeasurement> dict = new ConcurrentDictionary<string, OneMeasurement>();
        private ConcurrentDictionary<string, int> opStatus = new ConcurrentDictionary<string, int>();
        private Boolean initialized = false;
        private DateTime startTime;
        static object Lock = new object();


        public static Measurements GetMesMeasurements()
        {
            if (singleton == null)
            {
                singleton = new Measurements();
            }
            return singleton;
        }

        public void PrintSummary()
        {
            var endTime = DateTime.UtcNow;
            int total = 0;
            double totalExecutionTime = endTime.Subtract(startTime).TotalSeconds;
            foreach (var entry in dict)
            {
                total += entry.Value.GetCount();
            }
            if (totalExecutionTime == 0)
            {
                Console.WriteLine("No summary: Ran for less than a second");
                return;
            }
            double throughput = total/ totalExecutionTime;
            Console.WriteLine("Overall Summary");
            Console.WriteLine("Total Execution time " + totalExecutionTime + "s");
            Console.WriteLine("Throughput ops/s " + throughput);
            if (opStatus.ContainsKey("Success")) {
                Console.WriteLine("There were " + opStatus["Success"] +" of ops successful");
            }

            if (opStatus.ContainsKey("Failure")) {
                Console.WriteLine("There were " + opStatus["Failure"] +" of ops successful");

            }
            //Console.WriteLine("There were "+ successPercent +"% of ops successful");
            foreach (var entry in dict)
            {
                Console.WriteLine("Summary for " + entry.Key);
                Console.Write(entry.Value.GetSummary());
            }
        }

        private OneMeasurement getOneMeasurement(string operation)
        {
            OneMeasurement measurement = dict.GetOrAdd(operation,
                stats => new OneMeasurementRaw()
            );
            return measurement;

        }

        public void Measure(Status status, string operation, double latency)
        {
             switch (status)
                {
                    case Status.Failure:
                        opStatus.AddOrUpdate("Failure", 1, (s, i) => i + 1);
                        break;
                    case Status.Success:
                        opStatus.AddOrUpdate("Success", 1, (s, i) => i + 1);
                        break;
                    case Status.IncorrectRecordCount:
                        opStatus.AddOrUpdate("Incorrect record count", 1, (s, i) => i + 1);
                        break;
                    case Status.ValueMismatch:
                        opStatus.AddOrUpdate("Value mismatch", 1, (s, i) => i + 1);
                        break;
                }

                if (this.initialized == false)
                {
                    startTime = DateTime.UtcNow;
                    this.initialized = true;
                }
                OneMeasurement m = getOneMeasurement(operation);
                m.Measure(latency);
        }
    }
}
