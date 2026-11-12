using System.Collections.ObjectModel;
using KafkaStudio.App.ViewModels.DataSearch;
using KafkaStudio.App.ViewModels.Mvvm;
using KafkaStudio.App.ViewModels.Shared;
using KafkaStudio.Core.Messaging;
using KafkaStudio.Search;

namespace KafkaStudio.App.ViewModels.KeyChains;

/// <summary>
/// The Key Chain screen: define named chains (an ordered list of topics), then enter a message key and
/// press Start Analyze - each topic of the chain is read, in order, for a message with that key, and the
/// time the key reached each topic (and the gaps between them) is shown. Read-only: it never produces.
/// </summary>
public sealed class KeyChainViewModel : ScanTabViewModel
{
    private readonly SearchScopeViewModel _scope;

    public ObservableCollection<KeyChain> Chains { get; } = new();

    /// <summary>The topics of the chain being edited, in order.</summary>
    public ObservableCollection<string> ChainTopics { get; } = new();

    public ObservableCollection<ChainStepResult> Steps { get; } = new();

    public ObservableCollection<string> ConnectionNames => _scope.ConnectionNames;

    public IReadOnlyList<Choice<TimeSpan?>> RangeChoices { get; } = new[]
    {
        new Choice<TimeSpan?>(null, "All messages"),
        new Choice<TimeSpan?>(TimeSpan.FromMinutes(15), "Last 15 minutes"),
        new Choice<TimeSpan?>(TimeSpan.FromHours(1), "Last hour"),
        new Choice<TimeSpan?>(TimeSpan.FromHours(24), "Last 24 hours"),
        new Choice<TimeSpan?>(TimeSpan.FromDays(7), "Last 7 days")
    };

    public AsyncRelayCommand RefreshTopicsCommand => _scope.RefreshTopicsCommand;
    public AsyncRelayCommand AnalyzeCommand { get; }
    public RelayCommand NewChainCommand { get; }
    public RelayCommand SaveChainCommand { get; }
    public RelayCommand DeleteChainCommand { get; }
    public RelayCommand DuplicateChainCommand { get; }
    public MessageActions Actions { get; }
    public RelayCommand AddTopicCommand { get; }
    public RelayCommand<string> RemoveTopicCommand { get; }
    public RelayCommand<string> MoveUpCommand { get; }
    public RelayCommand<string> MoveDownCommand { get; }

    public KeyChainViewModel(AppState state) : this(state, new SearchScopeViewModel(state)) { }

    private KeyChainViewModel(AppState state, SearchScopeViewModel scope) : base(state, scope, _ => { })
    {
        _scope = scope;
        _selectedRange = RangeChoices[0];
        _scope.PropertyChanged += (_, e) =>
        {
            if (e.PropertyName == nameof(SearchScopeViewModel.SelectedConnection))
            {
                OnPropertyChanged(nameof(SelectedConnection));
                AnalyzeCommand?.RaiseCanExecuteChanged();
            }
        };

        AnalyzeCommand = new AsyncRelayCommand(AnalyzeAsync, CanAnalyze);
        NewChainCommand = new RelayCommand(NewChain);
        SaveChainCommand = new RelayCommand(SaveChain, () => !string.IsNullOrWhiteSpace(ChainName) && ChainTopics.Count > 0);
        DeleteChainCommand = new RelayCommand(DeleteChain, () => SelectedChain is not null);
        DuplicateChainCommand = new RelayCommand(DuplicateChain, () => SelectedChain is not null);
        PreviousMatchCommand = new RelayCommand(() => StepMatch(-1), () => _shown > 0);
        NextMatchCommand = new RelayCommand(() => StepMatch(1), () => SelectedStep is { } st && _shown < st.Count - 1);
        Actions = new MessageActions(state, () => _scope.SelectedConnection, status => Status = status);
        AddTopicCommand = new RelayCommand(AddTopic, () => !string.IsNullOrWhiteSpace(TopicToAdd));
        RemoveTopicCommand = new RelayCommand<string>(t => { if (t is not null) ChainTopics.Remove(t); });
        MoveUpCommand = new RelayCommand<string>(t => Move(t, -1));
        MoveDownCommand = new RelayCommand<string>(t => Move(t, 1));

        ChainTopics.CollectionChanged += (_, _) =>
        {
            AnalyzeCommand.RaiseCanExecuteChanged();
            SaveChainCommand.RaiseCanExecuteChanged();
            OnPropertyChanged(nameof(HasTopics));
        };

        foreach (var chain in KeyChainStore.Load()) Chains.Add(chain);
        SelectedChain = Chains.FirstOrDefault();
    }

    public string? SelectedConnection
    {
        get => _scope.SelectedConnection;
        set => _scope.SelectedConnection = value;
    }

    public bool HasTopics => ChainTopics.Count > 0;

    // ------------------------------------------------------------------ chains ----

    private KeyChain? _selectedChain;
    public KeyChain? SelectedChain
    {
        get => _selectedChain;
        set
        {
            if (!SetProperty(ref _selectedChain, value)) return;
            DeleteChainCommand?.RaiseCanExecuteChanged();
            DuplicateChainCommand?.RaiseCanExecuteChanged();
            if (value is null) return;
            ChainName = value.Name;
            ChainTopics.Clear();
            foreach (var t in value.Topics) ChainTopics.Add(t);
            ClearResult();
        }
    }

    private string _chainName = "";
    public string ChainName
    {
        get => _chainName;
        set { if (SetProperty(ref _chainName, value ?? "")) SaveChainCommand?.RaiseCanExecuteChanged(); }
    }

    private string? _topicToAdd;
    public string? TopicToAdd
    {
        get => _topicToAdd;
        set { if (SetProperty(ref _topicToAdd, value)) AddTopicCommand?.RaiseCanExecuteChanged(); }
    }

    private void NewChain()
    {
        _selectedChain = null;
        OnPropertyChanged(nameof(SelectedChain));
        DeleteChainCommand.RaiseCanExecuteChanged();
        DuplicateChainCommand.RaiseCanExecuteChanged();
        ChainName = "";
        ChainTopics.Clear();
        ClearResult();
        Status = "New chain - add topics in order, give it a name and Save.";
    }

    private void AddTopic()
    {
        var topic = TopicToAdd?.Trim();
        if (string.IsNullOrEmpty(topic)) return;
        if (TopicNames.Count > 0 && !TopicNames.Contains(topic))
        {
            Status = $"No topic named '{topic}' on this connection.";
            return;
        }
        if (ChainTopics.Contains(topic)) { Status = $"'{topic}' is already in the chain."; return; }
        ChainTopics.Add(topic);
        TopicToAdd = null;
    }

    private void Move(string? topic, int delta)
    {
        if (topic is null) return;
        var index = ChainTopics.IndexOf(topic);
        var target = index + delta;
        if (index < 0 || target < 0 || target >= ChainTopics.Count) return;
        ChainTopics.Move(index, target);
    }

    private void SaveChain()
    {
        var name = ChainName.Trim();
        if (name.Length == 0 || ChainTopics.Count == 0) return;
        var chain = new KeyChain { Name = name, Topics = ChainTopics.ToList() };

        // Same name (any case) as another chain than the one being edited would silently overwrite it.
        var clash = Chains.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase) && !ReferenceEquals(c, SelectedChain));
        if (clash is not null)
        {
            Status = $"A chain named '{clash.Name}' already exists - pick another name.";
            return;
        }

        var index = SelectedChain is null ? -1 : Chains.IndexOf(SelectedChain);
        if (index >= 0) Chains[index] = chain; else Chains.Add(chain);
        _selectedChain = chain;
        OnPropertyChanged(nameof(SelectedChain));
        DeleteChainCommand.RaiseCanExecuteChanged();
        DuplicateChainCommand.RaiseCanExecuteChanged();
        Persist($"Saved chain '{name}'.");
    }

    private void DuplicateChain()
    {
        if (SelectedChain is not { } chain) return;
        // Duplicating a copy numbers it ("Flow (copy 2)") instead of piling up suffixes.
        var baseName = System.Text.RegularExpressions.Regex.Replace(chain.Name, @" \(copy( \d+)?\)$", "");
        var name = $"{baseName} (copy)";
        for (var n = 2; Chains.Any(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase)); n++) name = $"{baseName} (copy {n})";
        var copy = new KeyChain { Name = name, Topics = chain.Topics.ToList() };
        Chains.Add(copy);
        SelectedChain = copy;
        Persist($"Duplicated '{chain.Name}' as '{name}' - rename it and save to keep your edits.");
    }

    private void DeleteChain()
    {
        if (SelectedChain is not { } chain) return;
        Chains.Remove(chain);
        var name = chain.Name;
        SelectedChain = Chains.FirstOrDefault();
        if (SelectedChain is null) NewChain();
        Persist($"Deleted chain '{name}'.");
    }

    private void Persist(string success)
    {
        var error = KeyChainStore.Save(Chains);
        Status = error is null ? success : $"Couldn't save chains: {error}";
    }

    // ----------------------------------------------------------------- analyze ----

    private string? _key;
    public string? Key
    {
        get => _key;
        set { if (SetProperty(ref _key, value)) AnalyzeCommand?.RaiseCanExecuteChanged(); }
    }

    private bool _caseSensitive;
    public bool CaseSensitive { get => _caseSensitive; set => SetProperty(ref _caseSensitive, value); }

    private Choice<TimeSpan?> _selectedRange;
    public Choice<TimeSpan?> SelectedRange
    {
        get => _selectedRange;
        set => SetProperty(ref _selectedRange, value ?? RangeChoices[0]);
    }

    private string? _summary;
    public string? Summary { get => _summary; private set => SetProperty(ref _summary, value); }

    private string? _verdict;
    /// <summary>"Sequence OK ..." / "Sequence broken at step N (topic) ..." / "Not found ..." for the last run.</summary>
    public string? Verdict { get => _verdict; private set { if (SetProperty(ref _verdict, value)) { OnPropertyChanged(nameof(IsSequenceOk)); OnPropertyChanged(nameof(IsSequenceBroken)); OnPropertyChanged(nameof(IsNotFound)); } } }

    private SequenceVerdict? _verdictKind;
    public bool IsSequenceOk => _verdictKind == SequenceVerdict.Ok;
    public bool IsSequenceBroken => _verdictKind == SequenceVerdict.Broken;
    public bool IsNotFound => _verdictKind == SequenceVerdict.NotFound;

    private bool? _complete;
    /// <summary>True when the key was found in every topic, false when something is missing, null before a run.</summary>
    public bool? LastRunComplete { get => _complete; private set => SetProperty(ref _complete, value); }

    private ChainStepResult? _selectedStep;
    public ChainStepResult? SelectedStep
    {
        get => _selectedStep;
        set
        {
            if (!SetProperty(ref _selectedStep, value)) return;
            _shown = value is { Found: true } ? Math.Max(0, value.Matches.ToList().IndexOf(value.First!)) : 0;
            RaiseMatchChanged();
        }
    }

    private int _shown;

    /// <summary>The message in the preview: the selected step's chosen message, or another match of the same topic (Prev/Next).</summary>
    public KafkaMessage? SelectedMessage =>
        SelectedStep is { Found: true } step ? step.Matches.ElementAtOrDefault(_shown) ?? step.First : null;

    public bool HasSeveralMatches => SelectedStep is { Count: > 1 };

    /// <summary>"2 of 3 matching messages in payments".</summary>
    public string? MatchPosition => SelectedStep is { Count: > 1 } step ? $"{_shown + 1} of {step.Count} matching messages in {step.Topic}" : null;

    public RelayCommand PreviousMatchCommand { get; }
    public RelayCommand NextMatchCommand { get; }

    private void StepMatch(int delta)
    {
        if (SelectedStep is not { } step) return;
        _shown = Math.Clamp(_shown + delta, 0, Math.Max(0, step.Count - 1));
        RaiseMatchChanged();
    }

    private void RaiseMatchChanged()
    {
        OnPropertyChanged(nameof(SelectedMessage));
        OnPropertyChanged(nameof(HasSeveralMatches));
        OnPropertyChanged(nameof(MatchPosition));
        PreviousMatchCommand?.RaiseCanExecuteChanged();
        NextMatchCommand?.RaiseCanExecuteChanged();
    }

    private bool CanAnalyze() => !IsRunning && !string.IsNullOrWhiteSpace(Key) && ChainTopics.Count > 0 && SelectedConnection is not null;

    protected override void OnRunningChanged() => AnalyzeCommand.RaiseCanExecuteChanged();

    private void ClearResult()
    {
        Steps.Clear();
        SelectedStep = null;
        Summary = null;
        _verdictKind = null;
        Verdict = null;
        LastRunComplete = null;
    }

    public async Task AnalyzeAsync()
    {
        var key = Key?.Trim();
        if (string.IsNullOrEmpty(key)) { Status = "Enter the text to look for (a key, an id, any word in the message)."; return; }
        if (ChainTopics.Count == 0) { Status = "Add at least one topic to the chain."; return; }
        if (SelectedConnection is null || !State.Connections.TryGetValue(SelectedConnection, out var gateway))
        {
            Status = "Connect to a cluster first.";
            return;
        }

        var chain = new KeyChain { Name = string.IsNullOrWhiteSpace(ChainName) ? "(unsaved chain)" : ChainName.Trim(), Topics = ChainTopics.ToList() };
        var range = SelectedRange.Value is { } last ? SearchRange.LastPeriod(last) : SearchRange.All;

        ClearResult();
        Status = $"Looking for '{key}' in {chain.Topics.Count} topic(s)…";
        await RunScanAsync(chain.Topics.Count, async (token, progress) =>
        {
            var result = await Task.Run(() => KeyChainAnalyzer.RunAsync(gateway, chain, key, range, token, progress, caseSensitive: CaseSensitive), token).ConfigureAwait(true);
            foreach (var step in result.Steps) Steps.Add(step);
            Summary = result.Summary;
            _verdictKind = result.Verdict;
            Verdict = result.VerdictText;
            LastRunComplete = result.Complete;
            Status = result.Details; // the verdict banner already shows the verdict
            SelectedStep = (result.BrokenStep is { Found: true } b ? b : null) ?? Steps.FirstOrDefault(s => s.Found);
        }).ConfigureAwait(true);
    }
}
