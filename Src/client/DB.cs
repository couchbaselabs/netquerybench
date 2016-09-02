using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Couchbase;
using Couchbase.Configuration.Client;
using Couchbase.Configuration.Server.Serialization;
using Couchbase.Core;
using Couchbase.IO;
using Couchbase.N1QL;
using netquerybench.measurements;

namespace netquerybench.client
{
    public class DB
    {
        private IBucket bucket;
        private string _bucketName;
        private Boolean _useKV;
        private Measurements _measurements;

        public void Init(string hostname, int port, string bucketName, string password, Boolean useKV, Measurements measurements)
        {
            var config = new ClientConfiguration();
            config.BucketConfigs.Remove("default");
            config.Servers.Clear();
            config.Servers.Add(
                new UriBuilder("http://", hostname, port, "pools").Uri);
            var couchbaseCluster = new Cluster(config);
            bucket = couchbaseCluster.OpenBucket(bucketName, password);
            _bucketName = bucketName;
            _useKV = useKV;
            _measurements = measurements;
        }

        public void Read(String table, String key, HashSet<String> fields, Dictionary<string, string> fieldValues)
        {
            var startTime = DateTime.UtcNow;
            Status status = read(table, key, fields, fieldValues);
            _measurements.Measure(status, "READ", (DateTime.UtcNow.Subtract(startTime)).TotalMilliseconds);
        }

        private Status read(String table, String key, HashSet<String> fields, Dictionary<string, string> fieldValues)
        {
            if (_useKV)
            {
                var result = bucket.Get<dynamic>(formatId(table, key));
                if (result.Status == ResponseStatus.Success)
                {
                    return Status.Success;
                }
                else
                {
                    return Status.Failure;
                }

            }
            else
            {
                string readquery = "SELECT " + joinSet(fields) + " FROM `" + _bucketName + "`"
                                       + " USE KEYS [$1]";
                var request = new QueryRequest(readquery).AddPositionalParameter(key);
                var result = bucket.Query<dynamic>(request);
                if (result.Status == QueryStatus.Success)
                {
                    return Status.Success;
                }
                else
                {
                    return Status.Failure;
                }
            }
        }

        public void Update(string table, string key, Dictionary<string, string> fields)
        {
            var startTime = DateTime.UtcNow;
            Status status = update(table, key, fields);
            _measurements.Measure(status, "UPDATE", (DateTime.UtcNow.Subtract(startTime)).TotalMilliseconds);
            
        }

        private Status update(String table, String key, Dictionary<string, string> fields)
        {

            if (_useKV)
            {
                var result = bucket.Upsert<dynamic>(formatId(table, key), fields);
                if (result.Status == ResponseStatus.Success)
                {
                    return Status.Success;
                }
                else
                {
                    return Status.Failure;
                }

            }
            else
            {
                String values = encodeN1QLFields(fields);
                string updateQuery = "UPDATE `" + _bucketName + "` USE KEYS [$1] SET " + values;
                var request = new QueryRequest(updateQuery).AddPositionalParameter(key);
                var result = bucket.Query<dynamic>(request);
                if (result.Status == QueryStatus.Success)
                {
                    return Status.Success;
                }
                else
                {
                    return Status.Failure;
                }
            }
            return Status.Success;
        }


        private string encodeN1QLFields(Dictionary<String, String> fields)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var entry in fields)
            {
                sb.Append(entry.Key);
                sb.Append("=");
                sb.Append("\"" + entry.Value + "\" ");
            }
            string ret = sb.ToString();
            return ret.Substring(0, ret.Length - 2);

        }

        public void Scan(String table, String key, int recordCount, HashSet<String> fields, Dictionary<string, string> fieldValues)
        {
            var startTime = DateTime.UtcNow;
            Status status = scan(table, key, recordCount, fields, fieldValues);
            _measurements.Measure(status, "SCAN", (DateTime.UtcNow.Subtract(startTime)).TotalMilliseconds);
        }

        private Status scan(String table, String key, int recordCount, HashSet<String> fields, Dictionary<string, string> fieldValues)
        {
            String scanQuery = "SELECT " + joinSet(fields) + " FROM `"
              + table + "` WHERE meta().id >= '$1' LIMIT $2";
            var request = new QueryRequest(scanQuery).AddPositionalParameter(key, recordCount);
            var result = bucket.Query<dynamic>(request);
            if (result.Status == QueryStatus.Success)
            {
                return Status.Success;
            }
            else
            {
                Console.WriteLine(result.Exception);
                return Status.Failure;
            }
            return Status.Success;
        }

        public void Insert(String table, String key, Dictionary<String, String> fields)
        {
            var startTime = DateTime.UtcNow;
            Status status = insert(table, key, fields);
            _measurements.Measure(status, "INSERT", (DateTime.UtcNow.Subtract(startTime)).TotalMilliseconds);
        }

        private Status insert(String table, String key, Dictionary<string, string> fields)
        {
            var result = bucket.Upsert<dynamic>(formatId(table, key), fields);
            if (result.Status == ResponseStatus.Success)
            {
                return Status.Success;
            }
            else
            {
                return Status.Failure;
            }
        }

        private string formatId(string prefix, string key)
        {
            return prefix + key;
        }

        private string joinSet(HashSet<String> fields)
        {
            String ret = null;
            if (fields.Count == 0)
            {
                ret = "*";
            }
            else
            {
                StringBuilder builder = new StringBuilder();
                foreach (string field in fields)
                {
                    builder.Append("`").Append(field).Append("`").Append(",");
                }
                String toReturn = builder.ToString();
                ret = toReturn.Substring(0, toReturn.Length - 1);
            }
            return ret;
        }


   
    }
}
