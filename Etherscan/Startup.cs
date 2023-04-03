using Etherscan.DAL.Services.DataServices;
using Etherscan.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Etherscan
{
    internal class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public virtual void ConfigureServices(IServiceCollection services)
        {
            services.Configure<BackgroundSettings>(this.Configuration)
                .AddOptions()
                .AddHostedService<IndexBlockService>()
                .AddTransient<BlockService>()
                .AddTransient<TransactionService>();
        }


        public void Configure(IApplicationBuilder app, ILoggerFactory loggerFactory)
        {
            app.UseRouting();
        }
    }
}
