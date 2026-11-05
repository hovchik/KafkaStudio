using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using KafkaStudio.Core.Messaging;

namespace KafkaStudio.Core.Display;

/// <summary>How a message value is shown. <see cref="Auto"/> keeps whatever the gateway decoded.</summary>
public enum ValueFormat
{
    Auto,
    Utf8,
    Base64,
    Hex,
    Int32,
    Int64,
    Float64
}

/// <summary>
/// Re-renders a message value from its exact bytes in a chosen format. Display only: the returned
/// message is a copy, the original (and what gets produced or rethrown) is never touched.
/// </summary>
public static class ValueFormatter
{
    public static IReadOnlyList<ValueFormat> All { get; } = Enum.GetValues<ValueFormat>();

    public static KafkaMessage Apply(KafkaMessage message, ValueFormat format)
    {
        if (format == ValueFormat.Auto || message.RawValue is null) return message;
        var text = Render(message.RawValue, format);
        return text is null ? message : message with { Value = text };
    }

    /// <summary>The text for these bytes in the given format, or null when they don't fit it.</summary>
    public static string? Render(byte[] bytes, ValueFormat format) => format switch
    {
        ValueFormat.Utf8 => Encoding.UTF8.GetString(bytes),
        ValueFormat.Base64 => Convert.ToBase64String(bytes),
        ValueFormat.Hex => Convert.ToHexString(bytes).ToLowerInvariant(),
        ValueFormat.Int32 when bytes.Length == 4 =>
            BinaryPrimitives.ReadInt32BigEndian(bytes).ToString(CultureInfo.InvariantCulture),
        ValueFormat.Int64 when bytes.Length == 8 =>
            BinaryPrimitives.ReadInt64BigEndian(bytes).ToString(CultureInfo.InvariantCulture),
        ValueFormat.Float64 when bytes.Length == 8 =>
            BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64BigEndian(bytes)).ToString("R", CultureInfo.InvariantCulture),
        _ => null
    };
}
