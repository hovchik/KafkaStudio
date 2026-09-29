using System.Threading.Channels;
using KafkaStudio.Core.Abstractions;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Scripting.Runtime;

/// <summary>
/// Backs a "watch topic" step. The critical correctness property here is that <see cref="StartAsync"/>
/// does not return until the underlying subscription is actually live - it does not wait for a message
/// to arrive, just for the gateway to report (via <see cref="ConsumeOptions.OnReady"/>) that its read
/// positions are pinned. That's what makes "Given watch topic B from now" followed immediately by
/// "When produce message to topic A" race-free for the cross-topic timing check: without this
/// guarantee, a fast reacting downstream system could produce to B before our subscription existed,
/// and we'd miss it.
///
/// History: an earlier version relied on the in-memory gateway registering its subscription
/// synchronously inside the first <c>MoveNextAsync()</c>. That held for the fake broker but not for a
/// real cluster, where assignment and offset lookup happen over the network - hence the explicit
/// readiness signal, which both gateways now raise.
/// </summary>
internal sealed class WatchHandle : IAsyncDisposable
{
    /// <summary>Upper bound on waiting for readiness, so a gateway that never signals can't hang a script.</summary>
    public static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(30);

    private readonly Channel<KafkaMessage> _channel;
    private readonly CancellationTokenSource _cts;
    private readonly Task _pumpTask;
    private int _disposed;

    public ChannelReader<KafkaMessage> Reader => _channel.Reader;

    private WatchHandle(Channel<KafkaMessage> channel, CancellationTokenSource cts, Task pumpTask)
    {
        _channel = channel;
        _cts = cts;
        _pumpTask = pumpTask;
    }

    public static async Task<WatchHandle> StartAsync(IKafkaGateway gateway, ConsumeOptions options, CancellationToken parentToken)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(parentToken);
        var channel = Channel.CreateUnbounded<KafkaMessage>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var readyOptions = options with
        {
            OnReady = () =>
            {
                options.OnReady?.Invoke();
                ready.TrySetResult();
            }
        };

        var pumpTask = Task.Run(() => PumpAsync(gateway, readyOptions, channel, ready, cts.Token));
        var handle = new WatchHandle(channel, cts, pumpTask);

        try
        {
            await ready.Task.WaitAsync(ReadyTimeout, parentToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // Carry on: the subscription may still become live, and an "expect" step will time out
            // with a clear message if nothing arrives.
        }
        catch
        {
            await handle.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return handle;
    }

    private static async Task PumpAsync(
        IKafkaGateway gateway,
        ConsumeOptions options,
        Channel<KafkaMessage> channel,
        TaskCompletionSource ready,
        CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            await foreach (var message in gateway.ConsumeAsync(options, cancellationToken).ConfigureAwait(false))
            {
                await channel.Writer.WriteAsync(message, cancellationToken).ConfigureAwait(false);
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
            ready.TrySetResult(); // never leave StartAsync waiting on a subscription that died
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _cts.Cancel();
        try
        {
            await _pumpTask.ConfigureAwait(false);
        }
        catch
        {
            // shutdown path, the pump's own try/catch already handled anything worth reporting
        }
        finally
        {
            _cts.Dispose();
        }
    }
}
