using System;
using System.Threading;
using netquerybench.client;
using netquerybench.workload;

namespace netquerybench
{
    public class Client
    {
        private Boolean _doTransactions = false;
        private Workload _workload = null;
        private int _operationCount = 0;
        private int _documentCount = 0;
        private DB _db;

        public Client(DB db, Boolean doTransactions, Workload workload, int operationCount, int documentCount)
        {
            _doTransactions = doTransactions;
            _workload = workload;
            _operationCount = operationCount;
            _documentCount = documentCount;
            _db = db;
        }

        public void Run()
        {
            int opscount = 0;
            int docscount = 0;
            while (opscount++ < _operationCount)
            {
                if (_doTransactions)
                {
                    _workload.DoTransaction(_db);

                }
                else
                {
                    if (docscount++ > _documentCount) break;
                    _workload.DoInsert(_db);
                    
                }
            }
            
        }

    }
}
