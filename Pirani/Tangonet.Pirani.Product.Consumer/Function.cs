using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using System.Text;
using Document = Amazon.DynamoDBv2.DocumentModel.Document;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace product_consumer_lambda_dev
{
    public class Function
    {
        private readonly IAmazonDynamoDB _dynamoDBClient;
        private readonly string _tableName;

        public Function()
        {
            _dynamoDBClient = new AmazonDynamoDBClient();
            _tableName = "aml-product-data-capture-audits";
        }

        public async Task FunctionHandler(KinesisEvent kinesisEvent, ILambdaContext context)
        {
            if (kinesisEvent == null || kinesisEvent.Records == null)
            {
                context.Logger.LogLine("Kinesis event or its records are null.");
                return;
            }

            foreach (var record in kinesisEvent.Records)
            {
                if (record == null || record.Kinesis == null || record.Kinesis.Data == null)
                {
                    context.Logger.LogLine("Record or its properties are null.");
                    continue;
                }

                var data = Encoding.UTF8.GetString(record.Kinesis.Data.ToArray());
                context.Logger.LogLine($"Decoded string: {data}");

                try
                {
                    var document = Document.FromJson(data);

                    // Check if the document contains "id" attribute, generate if missing
                    if (!document.Contains("id"))
                    {
                        // Generate unique id
                        var id = Guid.NewGuid().ToString();
                        document["id"] = id;
                    }

                    // Check if the document contains "user_id" attribute, generate if missing
                    if (!document.Contains("user_id"))
                    {
                        // Generate unique user_id
                        var userId = Guid.NewGuid().ToString();
                        document["user_id"] = userId;
                    }

                    // Get the partition key from the Kinesis record
                    var partitionKey = record.Kinesis.PartitionKey;

                    await SaveToDynamoDB(partitionKey, document);

                    await PostDataToEndpoint(data, context);
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine($"Error processing record: {ex.Message}");
                }
            }
        }

        private async Task SaveToDynamoDB(string partitionKey, Document document)
        {
            var table = Table.LoadTable(_dynamoDBClient, _tableName);

            var item = new Document();
            // Use partitionKey as the primary key
            item["PartitionKey"] = partitionKey;
            foreach (var attribute in document.GetAttributeNames())
            {
                item[attribute] = document[attribute];
            }

            await table.PutItemAsync(item);
        }

        private async Task PostDataToEndpoint(string data, ILambdaContext context) // Add ILambdaContext parameter
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("x-api-key", "PN.L6t5wbN6Mtj7.Z2n-5NzS2GkGqG9qwmKllwd-IC01u7kQRb5Flb_xAP9Nlwbl");

                    var content = new StringContent(data, Encoding.UTF8, "application/json");

                    var response = await httpClient.PostAsync("https://c2mwgtg0k0.execute-api.us-east-1.amazonaws.com/pirani-aml-stage/aml-api/entity/products", content);

                    response.EnsureSuccessStatusCode();
                    var responseContent = await response.Content.ReadAsStringAsync();

                    // Log message using context object
                    context.Logger.LogLine($"API Response: {responseContent}");
                    Console.WriteLine("API Response: " + responseContent);
                }
            }
            catch (Exception ex)
            {
                // Log error using context object
                context.Logger.LogLine("Error posting data to endpoint: " + ex.Message);
                Console.WriteLine("Error posting data to endpoint: " + ex.Message);
            }
        }
    }
}
