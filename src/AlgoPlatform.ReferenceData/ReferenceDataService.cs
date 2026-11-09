namespace AlgoPlatform.ReferenceData;

/// <summary>
/// Holds the host's current snapshot and publishes new versions. Readers take the reference once per
/// event (<see cref="Current"/> is a single volatile read); writers replace it atomically.
/// Strategy loops do not read this directly: the host turns every new version into a ReferenceData
/// event in each loop's inbox, so the swap happens between events and is journalled.
/// </summary>
public sealed class ReferenceDataService
{
    private readonly object _gate = new();
    private ReferenceDataSnapshot _current;

    public ReferenceDataService(ReferenceDataSnapshot initial) => _current = initial;

    public ReferenceDataSnapshot Current => Volatile.Read(ref _current);

    /// <summary>Raised on the publishing thread after the swap.</summary>
    public event Action<ReferenceDataSnapshot>? Changed;

    /// <summary>Publishes a new snapshot. Its version is forced above the current one.</summary>
    public ReferenceDataSnapshot Publish(ReferenceDataSnapshot next)
    {
        lock (_gate)
        {
            if (next.Version <= _current.Version) next = next.WithVersion(_current.Version + 1);
            Volatile.Write(ref _current, next);
        }
        Changed?.Invoke(next);
        return next;
    }

    public ReferenceDataSnapshot LoadAndPublish(string path) => Publish(ReferenceDataSnapshot.Load(path));
}
