using System;
using System.Collections.Generic;
using System.Threading;
using CommandLineParser.Arguments;
using netquerybench.client;
using netquerybench.measurements;
using netquerybench.workload;

namespace netquerybench
{
  class Options {
        [ValueArgument(typeof(int), 't', DefaultValue = 1, Description="Number of threads")]
        public int threads;

        [ValueArgument(typeof(int), 'd', DefaultValue = 1000, Description="Number of documents to be inserted in couchbase")] 
        public int documentcount;

        [ValueArgument(typeof(int), 'o', DefaultValue = 10000, Description="Number of operations. Run mode will run operations up to this count")]
        public int operationcount;

        [ValueArgument(typeof(int), 'f', DefaultValue = 10, Description="Number to fields inside json document")]
        public int fieldCount;

        [ValueArgument(typeof(int), 'l', DefaultValue = 100, Description="Json doc field length")]
        public int fieldLength;

        [ValueArgument(typeof(int), 'w', DefaultValue = 100, Description="Scan query limit")]
        public int scanLength;

        [ValueArgument(typeof(double), 'r', DefaultValue = 0, Description="Read proportion")]
        public double readratio;

        [ValueArgument(typeof(double), 'u', DefaultValue = 0, Description="Update proportion")] 
        public double updateratio;

        [ValueArgument(typeof(double), 's', DefaultValue = 1.0, Description="Scan proportion")] 
        public double scanratio;

        [ValueArgument(typeof(double), 'i',  DefaultValue = 0, Description="Insert proportion")]
        public double insertratio;

        [ValueArgument(typeof(string), 'm',  DefaultValue = "run", Description="Mode Load/Run")]
        public string mode;

        [ValueArgument(typeof(Boolean), 'k', DefaultValue = false, Description="Not supported yet")]
        public Boolean useKV;

        [ValueArgument(typeof(Boolean), 'a', DefaultValue = false, Description="Read all or subset of fields")]
        public Boolean readAllFields;

        [ValueArgument(typeof(string), 'c', DefaultValue = "192.168.1.66", Description="Couchbase server host name")]
        public string hostname;

        [ValueArgument(typeof(int), 'p', DefaultValue = 8091, Description="Couchbase server custom port to connect to")]
        public int port;

        [ValueArgument(typeof(string), 'b', DefaultValue = "default", Description="Couchbase bucket name")]
        public string bucketName;

        [ValueArgument(typeof(string), 'g', DefaultValue = "", Description="Couchbase bucket password")]
        public string bucketPassword;
  }


    class Program
    {
        private static readonly List<string> _showUsageCommands = new List<string> { "--help", "/?", "/help" };
        private static Workload getWorkload(Options opts)
        {
            var workload = new Workload();
            workload.FieldCount = opts.fieldCount;
            workload.Insertproportion = opts.insertratio;
            workload.UpdateProportion = opts.updateratio;
            workload.ScanProportion = opts.scanratio;
            workload.ReadProportion = opts.readratio;
            workload.ReadAllFields = opts.readAllFields;
            workload.Table = opts.bucketName;
            workload.FieldLength = opts.fieldLength;
            workload.ScanLength = opts.scanLength;
            workload.Init();
            return workload;
        }
        public static void Main(string[] args)
        {
            var parser = new CommandLineParser.CommandLineParser();
            var opts = new Options();
            parser.ExtractArgumentAttributes(opts);
            parser.ParseCommandLine(args);

            if (args.Length  == 0 ||  (args.Length == 1 && _showUsageCommands.Contains(args[0])))
            {
                return;
            }
            Workload workload = getWorkload(opts);
            Measurements measurements = Measurements.GetMesMeasurements();
            var clients = new List<Thread>();
            int opcount = opts.operationcount/opts.threads;

            for (int i = 0; i < opts.threads; i++)
            {
                DB db = new DB();
                try
                {
                    db.Init(opts.hostname, opts.port, opts.bucketName, opts.bucketPassword, opts.useKV, measurements);
                }
                catch (Exception ex)
                {
                    Console.Write(ex.ToString());
                    Environment.Exit(1);
                }
                Client client;
                if (opts.mode.Equals("load") == true)
                {
                    client = new Client(db, false, workload, opcount, opts.documentcount);
                }
                else
                {
                    client = new Client(db, true, workload, opcount, opts.documentcount);
                }
                Thread clientThread = new Thread(new ThreadStart(client.Run));
                clientThread.Start();
                clients.Add(clientThread);
            }

            foreach (var ct in clients)
            {
                ct.Join();
            }

            measurements.PrintSummary();
        }
    }
}
