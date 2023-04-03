namespace Etherscan
{
    public class BackgroundSettings
    {
        public string ConnectionString { get; set; }
        public int GracePeriodTime { get; set; }
        public int IndexStart { get; set; }
        public int BlockToProcess { get; set; }
        public string ApiKey { get; set; }
        public string ApiServer { get; set; }
    }
}
