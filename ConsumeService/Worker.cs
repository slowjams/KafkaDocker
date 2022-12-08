using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsumeService
{
   public class Worker : BackgroundService
   {
      private readonly ILogger<Worker> _logger;
      private readonly string topic = "simpletalk_topic";

      public Worker(ILogger<Worker> logger)
      {
         _logger = logger;
      }

      public Task StartAsync(CancellationToken cancellationToken)
      {
         var conf = new ConsumerConfig
         {
            GroupId = "st_consumer_group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
         };

         using (var builder = new ConsumerBuilder<Ignore,string>(conf).Build())
         {
            builder.Subscribe(topic);
            var cancelToken = new CancellationTokenSource();
            try
            {
               while (true)
               {
                  var consumer = builder.Consume(cancelToken.Token);
                  Console.WriteLine($"Message: {consumer.Message.Value} received from {consumer.TopicPartitionOffset}");
               }
            }
            catch (Exception)
            {
               builder.Close();
            }
         }
         return Task.CompletedTask;
      }
      public Task StopAsync(CancellationToken cancellationToken)
      {
         return Task.CompletedTask;
      }
      protected override async Task ExecuteAsync(CancellationToken stoppingToken)
      {
         //while (!stoppingToken.IsCancellationRequested)
         //{
         //   _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
         //   await Task.Delay(1000, stoppingToken);
         //}
         var conf = new ConsumerConfig
         {
            GroupId = "st_consumer_group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
         };

         using (var builder = new ConsumerBuilder<Ignore, string>(conf).Build())
         {
            builder.Subscribe(topic);
            var cancelToken = new CancellationTokenSource();
            try
            {
               while (true)
               {
                  var consumer = builder.Consume(cancelToken.Token);
                  Console.WriteLine($"Message: {consumer.Message.Value} received from {consumer.TopicPartitionOffset}");
               }
            }
            catch (Exception)
            {
               builder.Close();
            }
         }
      }
   }
}
