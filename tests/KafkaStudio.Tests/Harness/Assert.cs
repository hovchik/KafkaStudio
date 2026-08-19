namespace KafkaStudio.Tests.Harness;

public sealed class AssertionFailedException : Exception
{
    public AssertionFailedException(string message) : base(message)
    {
    }
}

/// <summary>
/// Tiny hand-rolled assertion library. KafkaStudio.Tests deliberately avoids xUnit/NUnit/MSTest
/// because those are NuGet packages, and this project needs to build and run with zero external
/// dependencies in environments (like the one this solution was authored in) that can't reach
/// nuget.org. On a normal developer machine you'd usually reach for xUnit instead - this harness is a
/// pragmatic substitute, not a recommendation against using a real test framework where you can.
/// </summary>
public static class Assert
{
    public static void True(bool condition, string message = "expected condition to be true")
    {
        if (!condition) throw new AssertionFailedException(message);
    }

    public static void False(bool condition, string message = "expected condition to be false")
    {
        if (condition) throw new AssertionFailedException(message);
    }

    public static void Equal<T>(T expected, T actual, string? message = null)
    {
        if (!Equals(expected, actual))
        {
            throw new AssertionFailedException(message ?? $"expected <{expected}> but was <{actual}>");
        }
    }

    public static void NotEqual<T>(T notExpected, T actual, string? message = null)
    {
        if (Equals(notExpected, actual))
        {
            throw new AssertionFailedException(message ?? $"expected value to differ from <{notExpected}>");
        }
    }

    public static void NotNull(object? value, string message = "expected non-null value")
    {
        if (value is null) throw new AssertionFailedException(message);
    }

    public static void Null(object? value, string message = "expected null value")
    {
        if (value is not null) throw new AssertionFailedException(message);
    }

    public static void Contains(string expectedSubstring, string actual, string? message = null)
    {
        if (!actual.Contains(expectedSubstring, StringComparison.Ordinal))
        {
            throw new AssertionFailedException(message ?? $"expected \"{actual}\" to contain \"{expectedSubstring}\"");
        }
    }

    public static async Task ThrowsAsync<TException>(Func<Task> action, string? message = null)
        where TException : Exception
    {
        try
        {
            await action();
        }
        catch (TException)
        {
            return;
        }
        catch (Exception ex)
        {
            throw new AssertionFailedException(
                message ?? $"expected {typeof(TException).Name} but caught {ex.GetType().Name}: {ex.Message}");
        }
        throw new AssertionFailedException(message ?? $"expected {typeof(TException).Name} but no exception was thrown");
    }

    public static void Throws<TException>(Action action, string? message = null) where TException : Exception
    {
        try
        {
            action();
        }
        catch (TException)
        {
            return;
        }
        catch (Exception ex)
        {
            throw new AssertionFailedException(
                message ?? $"expected {typeof(TException).Name} but caught {ex.GetType().Name}: {ex.Message}");
        }
        throw new AssertionFailedException(message ?? $"expected {typeof(TException).Name} but no exception was thrown");
    }
}
