using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.SystemTextJson;
using System.Globalization;
using System.Text.Json;

[assembly: LambdaSerializer(typeof(DefaultLambdaJsonSerializer))]

namespace user_producer_lambda_dev
{
    public class Function
    {
        private readonly IAmazonKinesis _kinesisClient;

        public Function()
        {
            _kinesisClient = new AmazonKinesisClient();
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
                    context.Logger.LogLine("STARTING THE FOREACH ARRAYS");

                    var record = eventItem.GetProperty("event");
                    context.Logger.LogLine($"Operation is not 'insert'. Skipping record.{record}");
                    if (record.GetProperty("operationType").GetString() == "insert")
                    {
                        var fullDocument = record.GetProperty("fullDocument");
                        var onfidoResult = fullDocument.GetProperty("onfidoResult");
                        var procD = fullDocument.GetProperty("procDate");

                        var output = onfidoResult.GetProperty("output");
                        // Map JSON data to C# model
                        var identification = new ClientInfo
                        {
                            identificationType = GetStringOrNull(output, "document_type"),
                            identificationNumber = GetStringOrNull(output, "document_number"),
                            personType = GetStringOrNull(output, "personType") ?? "1",
                            firstName = GetStringOrNull(output, "first_name"),
                            firstLastName = GetStringOrNull(output, "last_name"),
                            bomCountry = GetStringOrNull(output, "bomCountry") ?? "",
                            bomCity = GetStringOrNull(output, "bomCity") ?? "", 
                            sex = GetStringOrNull(output, "sex") ?? "1",
                            economicActivity = GetStringOrNull(output, "economicActivity") ?? "default", 
                            registrationDate = GetDateTimeOrDefault(procD, "$date"),
                            state = GetStringOrNull(output, "state") ?? "1", 
                            businessName = GetStringOrNull(output, "businessName") ?? "Rass", 
                            updateAt = GetDateTimeOrDefault(onfidoResult, "updatedDate")
                        };

                        // Serialize the object to JSON

                        var json = JsonSerializer.Serialize(identification);

                        // Put the data into Kinesis
                        await PushDataToKinesis(json, context);
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

        private string GetStringOrNull(JsonElement jsonElement, string propertyName)
        {
            if (jsonElement.TryGetProperty(propertyName, out var property))
            {
                return property.GetString();
            }
            return null;
        }

        private DateTime GetDateTimeOrDefault(JsonElement jsonElement, string propertyName)
        {
            if (jsonElement.TryGetProperty(propertyName, out var property))
            {
                string dateString = property.GetString();
                if (DateTime.TryParseExact(dateString, "yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
                {
                    return result;
                }
            }
            return DateTime.MinValue;
        }


        private async Task PushDataToKinesis(string data, ILambdaContext context)
        {
            try
            {
                var putRecordRequest = new PutRecordRequest
                {
                    StreamName = "user-data-stream",
                    PartitionKey = "partitionKey",
                    Data = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(data)) // Wrap the byte array in a MemoryStream
                };

                var response = await _kinesisClient.PutRecordAsync(putRecordRequest);

                context.Logger.LogLine($"Successfully put record into Kinesis. ShardId: {response.ShardId}");
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Failed to put record into Kinesis: {ex.Message}");
            }
        }

    }
}
