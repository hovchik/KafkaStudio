using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace KafkaStudio.App.ViewModels.Mvvm;

/// <summary>
/// Minimal INotifyPropertyChanged base class. Hand-rolled instead of taking a dependency on
/// CommunityToolkit.Mvvm so this whole ViewModels project stays free of NuGet packages and can be
/// built and unit tested without network access - see the solution README for why that matters here.
/// It covers the same 90% CommunityToolkit.Mvvm's [ObservableProperty]/SetProperty pattern covers.
/// </summary>
public abstract class ObservableObject : INotifyPropertyChanged
{
    public event PropertyChangedEventHandler? PropertyChanged;

    protected bool SetProperty<T>(ref T field, T value, [CallerMemberName] string? propertyName = null)
    {
        if (EqualityComparer<T>.Default.Equals(field, value)) return false;
        field = value;
        OnPropertyChanged(propertyName);
        return true;
    }

    protected void OnPropertyChanged([CallerMemberName] string? propertyName = null) =>
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
}
