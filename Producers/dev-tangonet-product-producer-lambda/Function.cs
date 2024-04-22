using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.SystemTextJson;
using System.Text;
using System.Text.Json;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using System.Globalization;

[assembly: LambdaSerializer(typeof(DefaultLambdaJsonSerializer))]

namespace dev_tangonet_product_producer_lambda
{
    public partial class Function
    {
        private readonly IAmazonKinesis _kinesisClient;
        private readonly string _streamName;

        public Function()
        {
            _kinesisClient = new AmazonKinesisClient();
            _streamName = "product-data-stream";
        }
        public async Task FunctionHandler(JsonDocument documentDbEvent, ILambdaContext context)
        {
            context.Logger.LogLine("STARTING THE FUNCTION");
            try
            {
                var eventsArray = documentDbEvent.RootElement.GetProperty("events");
                context.Logger.LogLine("STARTING THE EVENT ARRAYS");

                foreach (var eventItem in eventsArray.EnumerateArray())
                {
                    context.Logger.LogLine($"STARTING THE FOREACH ARRAYS{eventItem}");

                    var record = eventItem.GetProperty("event");
                    context.Logger.LogLine($"STARTING THE FOREACH ARRAYS{record}");
                    if (record.GetProperty("operationType").GetString() == "insert")
                    {
                        var fullDocument = record.GetProperty("fullDocument");

                        if (fullDocument.GetProperty("result").GetBoolean() == true)
                        {
                            var sendInfo = fullDocument.GetProperty("details").GetProperty("sendInfo");
                            var sendAmounts = sendInfo.GetProperty("sendAmounts");

                            var transactionD = fullDocument.TryGetProperty("transactionDate", out var transactionDate);
                            var transactionT = fullDocument.TryGetProperty("transactionType", out var transactionType);
                            var totalA = fullDocument.TryGetProperty("totalAmount", out var totalAmount);
                            var machineI = fullDocument.TryGetProperty("machineId", out var machineId);
                            var partnerI = fullDocument.TryGetProperty("partnerId", out var partnerId);
                            var senderCty = sendInfo.TryGetProperty("senderCity", out var senderCity);
                            var senderCtr = sendInfo.TryGetProperty("senderCity", out var senderCountry);
                            var totalSF = sendAmounts.TryGetProperty("totalSendFees", out var totalSendFees);
                            var sendCur = sendAmounts.TryGetProperty("sendCurrency", out var sendCurrency);
                            DateTime transactionDateTime = DateTime.ParseExact(transactionDate.GetString(), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);

                            // Format the DateTime object as per the desired output format
                            string formattedTransactionDate = transactionDateTime.ToString("yyyy-MM-ddTHH:mm:ss");

                            if (fullDocument.GetProperty("transactionDate").GetString() != "" &&
                            fullDocument.GetProperty("transactionType").GetString() != "" &&
                            fullDocument.GetProperty("totalAmount").GetString() != "" &&
                            fullDocument.GetProperty("machineId").GetString() != "" &&
                            fullDocument.GetProperty("partnerId").GetString() != "" &&
                            sendInfo.GetProperty("senderCity").GetString() != "" &&
                            sendInfo.GetProperty("senderCity").GetString() != "" &&
                            sendAmounts.GetProperty("totalSendFees").GetString() != "" &&
                            sendAmounts.GetProperty("sendCurrency").GetString() != "")
                            {
                                var ProductInfo = new ProductInfo
                                {
                                    Product = "default",
                                    Subproduct = "default",
                                    ProductState = "1",
                                    ProductNumber = "21",
                                    RegistrationDate=formattedTransactionDate,
                                    City="default",
                                    BranchOffice="default",
                                    DistributionChannel = "default",
                                    IdentificationType ="1",
                                    IdentificationNumber = "1",
                                    ParentType = "COUNTERPARTIES",
                                };

                                var productInfoJson = JsonSerializer.Serialize(ProductInfo);
                                context.Logger.LogLine($"Transaction Info: {productInfoJson}");

                                // Put the record into the Kinesis stream
                                await PushDataToKinesis(productInfoJson, context);

                                // Log success message
                                context.Logger.LogLine("Data sent to Kinesis successfully.");
                            }

                            else
                            {
                                context.Logger.LogLine("Required fields is empty.");
                            }
                        }
                        else
                        {
                            context.Logger.LogLine("Transaction status: ERROR.");
                        }
                    }
                    else
                    {
                        context.Logger.LogLine("Operation is not 'insert'. Skipping record.");
                    }
                }
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"An unexpected exception occurred: {ex}");
            }
        }

        private async Task PushDataToKinesis(string data, ILambdaContext context)
        {
            try
            {
                var request = new PutRecordRequest
                {
                    StreamName = _streamName,
                    PartitionKey = Guid.NewGuid().ToString(),
                    Data = new MemoryStream(Encoding.UTF8.GetBytes(data))
                };

                var response = await _kinesisClient.PutRecordAsync(request);

                if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    // Log success message
                    context.Logger.LogLine("Data sent to Kinesis successfully.");
                    context.Logger.LogLine($"Data sent to Kinesis successfully.{response.ShardId}");
                }
                else
                {
                    // Log failure message
                    context.Logger.LogLine($"Failed to send data to Kinesis. HTTP status code: {response.HttpStatusCode}");
                }
            }
            catch (Exception ex)
            {
                // Log exception
                context.Logger.LogLine($"An unexpected exception occurred while sending data to Kinesis: {ex}");
            }
        }
    }

}