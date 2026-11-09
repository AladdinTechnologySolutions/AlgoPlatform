using System.Globalization;
using System.Text;
using AlgoPlatform.Core.Timing;

namespace AlgoPlatform.Core.Metrics;

/// <summary>Builds Prometheus text exposition format. Control path only (runs on a scrape request).</summary>
public sealed class PrometheusWriter
{
    private readonly StringBuilder _sb = new();
    private readonly HashSet<string> _declared = new();

    public PrometheusWriter Counter(string name, string help, double value, params (string Key, string Value)[] labels) =>
        Sample(name, "counter", help, value, labels);

    public PrometheusWriter Gauge(string name, string help, double value, params (string Key, string Value)[] labels) =>
        Sample(name, "gauge", help, value, labels);

    /// <summary>Writes a histogram as a Prometheus summary in seconds (quantiles 0.5/0.99/0.999/1).</summary>
    public PrometheusWriter Summary(string name, string help, LatencyHistogram h, params (string Key, string Value)[] labels)
    {
        Declare(name, "summary", help);
        foreach (double q in new[] { 0.5, 0.99, 0.999 })
            Line(name, h.Percentile(q * 100) / 1e9, [.. labels, ("quantile", q.ToString(CultureInfo.InvariantCulture))]);
        Line(name, h.Max / 1e9, [.. labels, ("quantile", "1")]);
        Line(name + "_count", h.Count, labels);
        Line(name + "_sum", h.Mean * h.Count / 1e9, labels);
        return this;
    }

    private PrometheusWriter Sample(string name, string type, string help, double value, (string Key, string Value)[] labels)
    {
        Declare(name, type, help);
        Line(name, value, labels);
        return this;
    }

    private void Declare(string name, string type, string help)
    {
        if (!_declared.Add(name)) return;
        _sb.Append("# HELP ").Append(name).Append(' ').Append(help).Append('\n');
        _sb.Append("# TYPE ").Append(name).Append(' ').Append(type).Append('\n');
    }

    private void Line(string name, double value, (string Key, string Value)[] labels)
    {
        _sb.Append(name);
        if (labels.Length > 0)
        {
            _sb.Append('{');
            for (int i = 0; i < labels.Length; i++)
            {
                if (i > 0) _sb.Append(',');
                _sb.Append(labels[i].Key).Append("=\"").Append(labels[i].Value.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"');
            }
            _sb.Append('}');
        }
        _sb.Append(' ').Append(value.ToString("G17", CultureInfo.InvariantCulture)).Append('\n');
    }

    public override string ToString() => _sb.ToString();
}
