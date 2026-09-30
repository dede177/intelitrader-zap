using StackExchange.Redis;


var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect(
        $"{builder.Configuration["redis"] ?? "127.0.0.1:6379"},abortConnect=false"));
builder.Services.AddSingleton<MessageStore>();
builder.Services.AddHostedService<StreamConsumer>();

var app = builder.Build();

app.MapGet("/messages", (MessageStore store) => store.Snapshot());
app.MapGet("/health", (IConnectionMultiplexer redis) =>
    redis.IsConnected ? Results.Ok(new { status = "ok" }) : Results.StatusCode(503));

app.MapPost("/contacts", async (ContactRequest contact, IConnectionMultiplexer redis) =>
{
    if (!TryNormalizeContact(contact.Name, contact.Number, out var name, out var number))
        return Results.BadRequest(new { error = "number and name inavlid" });

    var commandId = await redis.GetDatabase().StreamAddAsync("android:contacts",
    [
        new NameValueEntry("name", name),
        new NameValueEntry("number", number)
    ]);

    return Results.Accepted(value: new { commandId = commandId.ToString(), status = "queued" });
});

app.Run();

static bool TryNormalizeContact(
    string? rawName,
    string? rawNumber,
    out string name,
    out string number)
{
    name = rawName?.Trim() ?? "";
    var candidate = rawNumber?.Trim() ?? "";
    var digits = candidate.StartsWith('+') ? candidate[1..] : candidate;


    if (name.Length == 0 || digits.Length is < 7 or > 15 ||
        digits.Any(digit => digit is < '0' or > '9'))
    {
        number = "";
        return false;
    }

    number = $"+{digits}";
    return true;
}


record ContactRequest(string? Name, string? Number);
record Message(long MessageId, long Timestamp, string ChatJid, string SenderJid, string Text);


sealed class MessageStore
{
    private readonly Queue<Message> messages = new();
    private readonly HashSet<long> ids = new();
    private readonly object gate = new();

    public void Add(Message message)
    {
        lock (gate)
        {
            if (!ids.Add(message.MessageId)) return;
            messages.Enqueue(message);
            if (messages.Count > 100) ids.Remove(messages.Dequeue().MessageId);
        }
    }

    public Message[] Snapshot()
    {
        lock (gate) return messages.ToArray();
    }
}


sealed class StreamConsumer(
    IConnectionMultiplexer redis,
    MessageStore store,
    ILogger<StreamConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        RedisValue position = "0-0";
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var entries = await redis.GetDatabase()
                    .StreamReadAsync("whatsapp:messages", position, count: 100);
                foreach (var entry in entries)
                {
                    if (long.TryParse(entry["message_id"].ToString(), out var id) &&
                        long.TryParse(entry["timestamp"].ToString(), out var timestamp))
                    {
                        var message = new Message(id, timestamp,
                            entry["chat_jid"].ToString(),
                            entry["sender_jid"].ToString(),
                            entry["text"].ToString());
                        store.Add(message);
                        logger.LogInformation(
                            "mensagem whatsapp {MessageId} at {Timestamp} from {SenderJid} in {ChatJid}: {Text}",
                            message.MessageId, message.Timestamp, message.SenderJid,
                            message.ChatJid, message.Text);
                    }
                    else logger.LogWarning("stream entry invalid {StreamId}", entry.Id);
                    position = entry.Id;
                }
                if (entries.Length < 100)
                    await Task.Delay(500, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception error)
            {
                logger.LogWarning(error, "stream read failed; retrying");
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
