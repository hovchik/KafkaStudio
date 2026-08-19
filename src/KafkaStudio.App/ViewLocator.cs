using Avalonia.Controls;
using Avalonia.Controls.Templates;
using KafkaStudio.App.ViewModels.Mvvm;

namespace KafkaStudio.App;

/// <summary>
/// Standard Avalonia ViewModel-first navigation convention: given a ViewModel instance, find the View
/// by naming convention (namespace "...ViewModels.Foo.BarViewModel" -> "...Views.Foo.BarView", same
/// assembly) and instantiate it. This is how <see cref="MainWindow"/>'s content area turns
/// <see cref="ViewModels.MainWindowViewModel.SelectedItem"/> into the right screen without a big
/// switch statement - registering a new ViewModel/View pair here just works by naming them
/// consistently.
/// </summary>
public sealed class ViewLocator : IDataTemplate
{
    public Control Build(object? param)
    {
        if (param is null) return new TextBlock { Text = "(no view model)" };

        var viewModelName = param.GetType().FullName!;
        var viewName = viewModelName.Replace(".ViewModels.", ".Views.").Replace("ViewModel", "View");
        var viewType = Type.GetType(viewName);

        if (viewType is not null && Activator.CreateInstance(viewType) is Control control)
        {
            return control;
        }

        return new TextBlock { Text = $"View not found: {viewName}" };
    }

    public bool Match(object? data) => data is ObservableObject;
}
