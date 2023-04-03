using Dapper;
using Etherscan.DAL.Entities.Data;
using Etherscan.DAL.Services.DataServices;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySql.Data.MySqlClient;
using System.IO;
using Etherscan.Extensions;
using Etherscan.Models;
using System.Diagnostics;
using Newtonsoft.Json;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Etherscan.Services
{
    public class IndexBlockService : BackgroundService
    {
        private readonly ILogger<IndexBlockService> _logger;
        private readonly BackgroundSettings _settings;
        private readonly BlockService _blockService;
        private readonly TransactionService _transactionService;

        public IndexBlockService(IOptions<BackgroundSettings> settings, ILogger<IndexBlockService> logger, BlockService blockService, TransactionService transactionService)
        {
            _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _blockService = blockService;
            _transactionService = transactionService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogDebug("IndexBlockService is starting.");

            stoppingToken.Register(() => _logger.LogDebug("IndexBlockService background task is stopping."));

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogDebug("IndexBlockService background task is doing background work.");
                await PerformTask();
                try
                {
                    await Task.Delay(_settings.GracePeriodTime, stoppingToken);
                }
                catch (TaskCanceledException exception)
                {
                    _logger.LogCritical(exception, "TaskCanceledException Error", exception.Message);
                }
            }

            _logger.LogDebug("IndexBlockService background task is stopping.");
        }

        public async Task PerformTask()
        {
            for (int blockNo = _settings.IndexStart; blockNo < _settings.IndexStart + _settings.BlockToProcess; blockNo++)
            {
                _logger.LogDebug($"Start perform block {blockNo}");
                var block = await GetBlockByNumber(blockNo);
                
                if(block == null) continue;

                await InsertBlock(block);
                var blockId = await _blockService.GetIdByBlockNumber(blockNo, _settings.ConnectionString);
                var transCount = await GetTransactionCount(blockNo);

                if(transCount <= 0) continue;

                for (int i = 0; i < transCount; i++)
                {
                    var tran = await GetTransaction(blockNo, i);
                    if(tran == null) continue;
                    tran.BlockId = blockId;
                    await InsertTrans(tran);
                }

            }
        }

        private async Task<Block> GetBlockByNumber(int blockNumber)
        {
            var client = new HttpClient();
            var path = $"{_settings.ApiServer}?module=proxy&action=eth_getBlockByNumber&tag={blockNumber.ToHex()}&boolean=false&apikey={_settings.ApiKey}"; 
            HttpResponseMessage response = await client.GetAsync(path);
            if (response.IsSuccessStatusCode)
            {
                var resp =  await response.Content.ReadAsAsync<EtherscanCommonResp<GetBlockByNumberResp>>();
                var block = ConvertBlock(resp.result);
                block.BlockNumber = blockNumber;
                return block;
            }
            return null;
        }

        public async Task<int> GetTransactionCount(int blockNumber)
        {
            var client = new HttpClient();
            var path = $"{_settings.ApiServer}?module=proxy&action=eth_getBlockTransactionCountByNumber&tag={blockNumber.ToHex()}&apikey={_settings.ApiKey}";
            HttpResponseMessage response = await client.GetAsync(path);
            if (response.IsSuccessStatusCode)
            {
                var resp = await response.Content.ReadAsAsync<EtherscanCommonResp<string>>();
                return resp.result.FromHex();
            }

            return 0;
        }

        public async Task<Transaction> GetTransaction(int blockNumber, int index)
        {
            var timer = new Stopwatch();
            timer.Start();

            var client = new HttpClient();
            var path = $"{_settings.ApiServer}?module=proxy&action=eth_getTransactionByBlockNumberAndIndex&tag={blockNumber.ToHex()}&index={index.ToHex()}&apikey={_settings.ApiKey}";
            HttpResponseMessage response = await client.GetAsync(path);
            Transaction tran = null;
            if (response.IsSuccessStatusCode)
            {
                var resp = await response.Content.ReadAsAsync<EtherscanCommonResp<GetTransactionFromBlockNumberIndexResp>>();
                tran = ConvertTransaction(resp.result);
            }

            timer.Stop();
            _logger.LogInformation($"Get transaction block {blockNumber} index {index} FROM API server with {timer.ElapsedMilliseconds/1000} seconds {JsonSerializer.Serialize(tran)}");

            return tran;
        }



        public Block ConvertBlock(GetBlockByNumberResp model)
        {
            return new Block
            {
                Hash = model.hash,
                ParentHash = model.parentHash,
                BlockReward = model.number.FromHexDecimal(),
                GasLimit = model.gasLimit.FromHexDecimal(),
                GasUsed = model.gasUsed.FromHexDecimal(),
                Miner = model.miner
            };
        }

        public Transaction ConvertTransaction(GetTransactionFromBlockNumberIndexResp model)
        {
            return new Transaction
            {
                Hash = model.hash,
                From = model.from,
                To = model.to,
                Value = model.value.FromHexDecimal(),
                Gas = model.gas.FromHexDecimal(),
                GasPrice = model.gasPrice.FromHexDecimal(),
                TransactionIndex = model.transactionIndex.FromHex()

            };
        }

        private async Task<int> InsertBlock(Block block)
        {
            return await _blockService.Insert(block, _settings.ConnectionString);
        }

        private async Task<int> InsertTrans(Transaction tran)
        {
            return await _transactionService.Insert(tran, _settings.ConnectionString);
        }
    }
}
