using Etherscan.DAL.Entities.Data;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using static Dapper.SqlMapper;

namespace Etherscan.DAL.Services.DataServices
{
    public class BlockService
    {
        private readonly ILogger<BlockService> _logger;
        public BlockService(ILogger<BlockService> logger)
        {
            _logger = logger;
        }

        public async Task<int> Insert(Block entity, string connString)
        {
            using var conn = new MySqlConnection(connString);
            try
            {
                conn.Open();
                var query = @"
                                INSERT INTO blocks (blockNumber, hash, parentHash, miner, blockReward, gasLimit, gasUsed)
                                VALUES (@blockNumber, @hash, @parentHash, @miner, @blockReward, @gasLimit, @gasUsed);
                            ";
                MySqlCommand m = new MySqlCommand(query);
                m.Connection = conn;
                m.Parameters.AddWithValue("@blockNumber", entity.BlockNumber);
                m.Parameters.AddWithValue("@hash", entity.Hash);
                m.Parameters.AddWithValue("@parentHash", entity.ParentHash);
                m.Parameters.AddWithValue("@miner", entity.Miner);
                m.Parameters.AddWithValue("@blockReward", entity.BlockReward);
                m.Parameters.AddWithValue("@gasLimit", entity.GasLimit);
                m.Parameters.AddWithValue("@gasUsed", entity.GasUsed);

                return await m.ExecuteNonQueryAsync();
            }
            catch (Exception exception)
            {
                _logger.LogCritical(exception, "FATAL ERROR: Database connections could not be opened: {Message}", exception.Message);
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }

        public async Task<int> GetIdByBlockNumber(int blockNumber, string connString)
        {
            using var conn = new MySqlConnection(connString);
            try
            {
                conn.Open();
                var query = $@"
                                SELECT blockId FROM blocks WHERE blockNumber = {blockNumber};
                            ";
                return conn.ExecuteScalar<int>(query);
            }
            catch (Exception exception)
            {
                _logger.LogCritical(exception, "FATAL ERROR: Database connections could not be opened: {Message}", exception.Message);
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}
