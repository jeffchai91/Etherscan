namespace Etherscan.DAL.Entities.Data
{
    public class Block
    {
        public int BlockId { get; set; }
        public int BlockNumber { get; set; }
        public string Hash { get; set; }
        public string ParentHash { get; set; }
        public string Miner { get; set; }
        public decimal BlockReward { get; set; }
        public decimal GasLimit { get; set; }
        public decimal GasUsed { get; set; }

    }
}
