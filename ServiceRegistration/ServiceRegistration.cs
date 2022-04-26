using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MongoSourceConnectorToEventGrid.ServiceRegistration
{
    public interface IServiceRegistration
	{
		void Configure(IServiceCollection services, IConfiguration configuration);
	}
}
