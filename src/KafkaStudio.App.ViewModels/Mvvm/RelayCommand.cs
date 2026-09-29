using System.Windows.Input;

namespace KafkaStudio.App.ViewModels.Mvvm;

/// <summary>
/// Last line of defence for exceptions thrown by command handlers. ICommand.Execute is necessarily
/// "fire and forget" (async void for the async commands), so an exception escaping a handler would
/// otherwise crash the whole app. The hosting app subscribes to show it to the user instead.
/// </summary>
public static class CommandErrors
{
    public static event Action<Exception>? Unhandled;

    internal static void Report(Exception ex)
    {
        try { Unhandled?.Invoke(ex); }
        catch { /* never let error reporting itself crash the app */ }
    }
}

/// <summary>Synchronous ICommand, hand-rolled for the same zero-dependency reason as
/// <see cref="ObservableObject"/> (see its doc comment).</summary>
public sealed class RelayCommand : ICommand
{
    private readonly Action _execute;
    private readonly Func<bool>? _canExecute;

    public RelayCommand(Action execute, Func<bool>? canExecute = null)
    {
        _execute = execute;
        _canExecute = canExecute;
    }

    public event EventHandler? CanExecuteChanged;

    public bool CanExecute(object? parameter) => _canExecute?.Invoke() ?? true;

    public void Execute(object? parameter)
    {
        if (!CanExecute(parameter)) return;
        try
        {
            _execute();
        }
        catch (Exception ex)
        {
            CommandErrors.Report(ex);
        }
    }

    public void RaiseCanExecuteChanged() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);
}

/// <summary>Parameterized synchronous ICommand.</summary>
public sealed class RelayCommand<T> : ICommand
{
    private readonly Action<T?> _execute;
    private readonly Func<T?, bool>? _canExecute;

    public RelayCommand(Action<T?> execute, Func<T?, bool>? canExecute = null)
    {
        _execute = execute;
        _canExecute = canExecute;
    }

    public event EventHandler? CanExecuteChanged;

    public bool CanExecute(object? parameter) => _canExecute?.Invoke(Cast(parameter)) ?? true;

    public void Execute(object? parameter)
    {
        if (!CanExecute(parameter)) return;
        try
        {
            _execute(Cast(parameter));
        }
        catch (Exception ex)
        {
            CommandErrors.Report(ex);
        }
    }

    public void RaiseCanExecuteChanged() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);

    // A binding can hand us a parameter of the wrong type while templates are being (re)applied;
    // treat that as "no parameter" instead of throwing an InvalidCastException.
    private static T? Cast(object? parameter) => parameter is T value ? value : default;
}

/// <summary>Async ICommand with re-entrancy guarded via <see cref="IsRunning"/>, so a slow Kafka call
/// bound to a button can't be fired twice by an impatient double-click. Commands whose handler cancels
/// its own previous run (e.g. "load this topic" superseding the previous load) pass
/// <c>allowConcurrentExecutions: true</c> so a new request isn't blocked by the one it supersedes.</summary>
public sealed class AsyncRelayCommand : ICommand
{
    private readonly Func<Task> _execute;
    private readonly Func<bool>? _canExecute;
    private readonly bool _allowConcurrentExecutions;
    private int _running;

    public AsyncRelayCommand(Func<Task> execute, Func<bool>? canExecute = null, bool allowConcurrentExecutions = false)
    {
        _execute = execute;
        _canExecute = canExecute;
        _allowConcurrentExecutions = allowConcurrentExecutions;
    }

    public event EventHandler? CanExecuteChanged;

    public bool IsRunning => _running > 0;

    public bool CanExecute(object? parameter) =>
        (_allowConcurrentExecutions || _running == 0) && (_canExecute?.Invoke() ?? true);

    public async void Execute(object? parameter) => await ExecuteAsync();

    /// <summary>Awaitable execution (what <see cref="Execute"/> does), for tests and keyboard shortcuts.</summary>
    public async Task ExecuteAsync()
    {
        if (!CanExecute(null)) return;
        _running++;
        RaiseCanExecuteChanged();
        try
        {
            await _execute().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            CommandErrors.Report(ex);
        }
        finally
        {
            _running--;
            RaiseCanExecuteChanged();
        }
    }

    public void RaiseCanExecuteChanged() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);
}

/// <summary>Parameterized async ICommand, e.g. bound to a per-row "Remove" button in a list.</summary>
public sealed class AsyncRelayCommand<T> : ICommand
{
    private readonly Func<T?, Task> _execute;
    private readonly Func<T?, bool>? _canExecute;
    private readonly bool _allowConcurrentExecutions;
    private int _running;

    public AsyncRelayCommand(Func<T?, Task> execute, Func<T?, bool>? canExecute = null, bool allowConcurrentExecutions = false)
    {
        _execute = execute;
        _canExecute = canExecute;
        _allowConcurrentExecutions = allowConcurrentExecutions;
    }

    public event EventHandler? CanExecuteChanged;

    public bool IsRunning => _running > 0;

    public bool CanExecute(object? parameter) =>
        (_allowConcurrentExecutions || _running == 0) && (_canExecute?.Invoke(Cast(parameter)) ?? true);

    public async void Execute(object? parameter) => await ExecuteAsync(Cast(parameter));

    public async Task ExecuteAsync(T? parameter)
    {
        if (!CanExecute(parameter)) return;
        _running++;
        RaiseCanExecuteChanged();
        try
        {
            await _execute(parameter).ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            CommandErrors.Report(ex);
        }
        finally
        {
            _running--;
            RaiseCanExecuteChanged();
        }
    }

    public void RaiseCanExecuteChanged() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);

    private static T? Cast(object? parameter) => parameter is T value ? value : default;
}
