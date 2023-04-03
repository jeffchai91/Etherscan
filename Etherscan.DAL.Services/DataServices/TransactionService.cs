using Etherscan.DAL.Entities.Data;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;

namespace Etherscan.DAL.Services.DataServices
{
    public class TransactionService
    {
        private readonly ILogger<TransactionService> _logger;
        public TransactionService(ILogger<TransactionService> logger)
        {
            _logger = logger;
        }

        public async Task<int> Insert(Transaction entity, string connString)
        {
            using var conn = new MySqlConnection(connString);
            try
            {
                conn.Open();
                var query = @"
                                   INSERT INTO transactions  (blockID, hash, `from`, `to`, value, gas, gasPrice, transactionIndex)
                                   VALUES (@blockId, @hash, @from, @to, @value, @gas, @gasPrice, @transactionIndex);
                            ";
                MySqlCommand m = new MySqlCommand(query);
                m.Connection = conn;
                m.Parameters.AddWithValue("@blockId", entity.BlockId);
                m.Parameters.AddWithValue("@hash", entity.Hash);
                m.Parameters.AddWithValue("@from", entity.From);
                m.Parameters.AddWithValue("@to", entity.To);
                m.Parameters.AddWithValue("@value", entity.Value);
                m.Parameters.AddWithValue("@gas", entity.Gas);
                m.Parameters.AddWithValue("@gasPrice", entity.GasPrice);
                m.Parameters.AddWithValue("@transactionIndex", entity.TransactionIndex);

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
    }
}
