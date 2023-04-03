using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Etherscan.Models
{
    public class EtherscanCommonResp<T>
    {
        public string jsonrpc { get; set; }
        public string id { get; set; }
        public T result { get; set; }
    }

    public class GetBlockByNumberResp
    {
        public string hash { get; set; }
        public string parentHash { get; set; }
        public string gasLimit { get; set; }
        public string gasUsed { get; set; }
        public string miner { get; set; }
        public string number { get; set; }
    }

    public class GetTransactionFromBlockNumberIndexResp
    {
        public string blockNumber { get; set; }
        public string from { get; set; }
        public string to { get; set; }
        public string gas { get; set; }
        public string gasPrice { get; set; }
        public string hash { get; set; }
        public string value { get; set; }
        public string transactionIndex { get; set; }
    }

}
