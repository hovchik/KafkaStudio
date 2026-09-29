using System.Collections.ObjectModel;

namespace KafkaStudio.App.ViewModels.Shared;

public static class CollectionSync
{
    /// <summary>
    /// Makes <paramref name="target"/> contain exactly <paramref name="source"/> (sorted), mutating it in
    /// place. Unlike Clear()+Add, items that stay keep their identity - so a ComboBox bound to the
    /// collection doesn't lose its selection (and push null into the ViewModel) every time an unrelated
    /// connection is added or removed.
    /// </summary>
    public static void SyncSorted(ObservableCollection<string> target, IEnumerable<string> source)
    {
        var desired = source.Distinct(StringComparer.Ordinal).OrderBy(s => s, StringComparer.OrdinalIgnoreCase).ToList();
        var desiredSet = new HashSet<string>(desired, StringComparer.Ordinal);

        for (var i = target.Count - 1; i >= 0; i--)
        {
            if (!desiredSet.Contains(target[i])) target.RemoveAt(i);
        }

        for (var i = 0; i < desired.Count; i++)
        {
            if (i < target.Count && target[i] == desired[i]) continue;
            var existing = target.IndexOf(desired[i]);
            if (existing >= 0) target.Move(existing, i);
            else target.Insert(i, desired[i]);
        }
    }
}
