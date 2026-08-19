using System.Threading.Channels;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Backs a "watch topic" step. The critical correctness property here is that <see cref="Start"/>
/// does not return until the underlying subscription is actually registered with the gateway - it
/// does not wait for a message to arrive, just for the act of subscribing to have happened. That's
/// what makes "Given watch topic B from now" followed immediately by "When produce message to topic A"
/// race-free for the cross-topic timing check: without this guarantee, a fast reacting downstream
/// system could produce to B before our subscription existed, and we'd miss it.
///
/// The trick: getting an <see cref="IAsyncEnumerator{T}"/> and calling <c>MoveNextAsync()</c> on it
/// runs the async iterator's body *synchronously* up to its first genuine suspension point (the point
/// where it's actually waiting for the next message) - so calling that here, before handing off to a
/// background pump task, is enough. No artificial delay, no polling.
/// </summary>
internal sealed class WatchHandle : IAsyncDisposable
{
    private readonly Channel<KafkaMessage> _channel;
    private readonly CancellationTokenSource _cts;
    private readonly Task _pumpTask;

    public ChannelReader<KafkaMessage> Reader => _channel.Reader;

    private WatchHandle(Channel<KafkaMessage> channel, CancellationTokenSource cts, Task pumpTask)
    {
        _channel = channel;
        _cts = cts;
        _pumpTask = pumpTask;
    }

    public static WatchHandle Start(IKafkaGateway gateway, ConsumeOptions options, CancellationToken parentToken)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(parentToken);
        var channel = Channel.CreateUnbounded<KafkaMessage>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        var enumerator = gateway.ConsumeAsync(options, cts.Token).GetAsyncEnumerator(cts.Token);

        // Deliberately not awaited here: invoking MoveNextAsync() runs the gateway's subscribe logic
        // synchronously before this call returns, which is the whole point - see the class doc comment.
        var firstMove = enumerator.MoveNextAsync();

        var pumpTask = Task.Run(() => PumpAsync(enumerator, firstMove, channel, cts.Token));

        return new WatchHandle(channel, cts, pumpTask);
    }

    private static async Task PumpAsync(
        IAsyncEnumerator<KafkaMessage> enumerator,
        ValueTask<bool> firstMove,
        Channel<KafkaMessage> channel,
        CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            var hasCurrent = await firstMove.ConfigureAwait(false);
            while (hasCurrent)
            {
                await channel.Writer.WriteAsync(enumerator.Current, cancellationToken).ConfigureAwait(false);
                hasCurrent = await enumerator.MoveNextAsync().ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // expected on Stop/Dispose
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            channel.Writer.TryComplete(failure);
            try { await enumerator.DisposeAsync().ConfigureAwait(false); }
            catch { /* best effort - we're already tearing down */ }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try
        {
            await _pumpTask.ConfigureAwait(false);
        }
        catch
        {
            // shutdown path, the pump's own try/catch already handled/logged anything worth losing
        }
        finally
        {
            _cts.Dispose();
        }
    }
}
