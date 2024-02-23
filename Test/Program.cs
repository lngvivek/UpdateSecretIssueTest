using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace example;

public class KeyCloak
{
    
    private static async Task<string> NewAccessToken()
    {
        var data = new[]
        {
            new KeyValuePair<string?, string?>("client_id", "producer"),
            new KeyValuePair<string?, string?>("client_secret", "kbOFBXI9tANgKUq8vXHLhT6YhbivgXxn"),
            new KeyValuePair<string?, string?>("grant_type", "client_credentials"),
        };
        var httpRequestMessage = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            RequestUri = new Uri("http://localhost:8080/realms/test/protocol/openid-connect/token"),
            Headers =
            {
                {
                    HttpRequestHeader.ContentType.ToString(), "application/x-www-form-urlencoded"
                },
            },
            Content = new FormUrlEncodedContent(data)
        };
        var client = new HttpClient();
        var response = await client.SendAsync(httpRequestMessage);
        var responseString = await response.Content.ReadAsStringAsync();
        var json = System.Text.Json.JsonDocument.Parse(responseString);
        var r = json.RootElement.GetProperty("access_token").GetString();
        return r ?? throw new Exception("no access token");
    }
    

    public static async Task Main(string[] args)
    {               
        var serviceCollection = new ServiceCollection();                            
        var accessToken = await NewAccessToken();
        Console.WriteLine(accessToken);
        serviceCollection.AddLogging(builder => builder
            .AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "[HH:mm:ss] ";
                options.ColorBehavior = LoggerColorBehavior.Default;
            })
            .AddFilter(level => level >= LogLevel.Debug)
        );
        var loggerFactory = serviceCollection.BuildServiceProvider()
            .GetService<ILoggerFactory>();

        if (loggerFactory != null)
        {
            var producerLogger = loggerFactory.CreateLogger<Producer>();
            var consumerLogger = loggerFactory.CreateLogger<Consumer>();
            StreamSystem system=null;
            try
            {
                system = await StreamSystem.Create(new StreamSystemConfig()
                {
                    AuthMechanism = AuthMechanism.Plain,
                    Ssl = new SslOption()
                    {
                        Enabled = true,
                        Version = System.Security.Authentication.SslProtocols.Tls12,
                        AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch |
                                                 SslPolicyErrors.RemoteCertificateChainErrors

                    },
                    UserName = "guest",
                    Password = accessToken,
                    VirtualHost = "test",  //set your virtual host
                    Endpoints = new List<EndPoint>() { new DnsEndPoint("Host", 5551) }  //Change host Name
                                        
                }, loggerFactory.CreateLogger<StreamSystem>());
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
            }

            //This will be super stream with three partition, please create using below
            //   add_super_stream test-keycloak --partitions 3 --max-age PT10M30S --vhost test
            const string stream = "test-keycloak";
            

            var start = DateTime.Now;
            var completed = new TaskCompletionSource<bool>();

            _ = Task.Run(async () =>
            {
                while (completed.Task.Status != TaskStatus.RanToCompletion)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    if (start.AddSeconds(180) >= DateTime.Now) continue;
                    Console.WriteLine($"{DateTime.Now} - Updating the secret....");
                    try
                    {
                        await system.UpdateSecret(await NewAccessToken()).ConfigureAwait(false);
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                    start = DateTime.Now;
                }
            });


            var consumer = await Consumer.Create(new ConsumerConfig(system, stream) 
            {
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = (_, _, _, message) =>
                {
                    Console.WriteLine(
                        $"{DateTime.Now} - Received: {Encoding.UTF8.GetString(message.Data.Contents.ToArray())} ");
                    return Task.CompletedTask;
                },
                IsSingleActiveConsumer=true,
                IsSuperStream=true,
                Reference="test"
            });


            var producer = await Producer.Create(new ProducerConfig(system, stream)
            {
                ClientProvidedName = "test",
                SuperStreamConfig = new SuperStreamConfig()
                {
                    Routing = msg => msg.ToString().Length.ToString(),
                    Enabled = true,
                },
            }); ;
            for (var i = 0; i < 10 * 60; i++)
            {
                await producer.Send(new Message(Encoding.UTF8.GetBytes($"Hello KeyCloak! {i}")));
                await Task.Delay(TimeSpan.FromSeconds(20));
                Console.WriteLine($"{DateTime.Now} - Sent: Hello KeyCloak! {i}");
            }

            completed.SetResult(true);
            Console.WriteLine("Closing...");
            await consumer.Close();
            await producer.Close();
            await system.Close();
            Console.WriteLine("Closed.");
        }
    }
}