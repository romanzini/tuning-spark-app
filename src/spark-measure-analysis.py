import json
import pandas as pd


def analyze_stage_metrics(file_path):
    stage_metrics = []
    with open(file_path) as f:
        for line in f:
            stage_metrics.append(json.loads(line.strip()))

    high_exec_time_stages = []
    high_gc_time_stages = []
    high_shuffle_time_stages = []

    for metric in stage_metrics:
        stage_id = metric.get("stageId")
        executor_run_time = metric.get("executorRunTime", 0)
        jvm_gc_time = metric.get("jvmGCTime", 0)
        shuffle_read_time = metric.get("shuffleFetchWaitTime", 0)
        shuffle_write_time = metric.get("shuffleWriteTime", 0)

        if executor_run_time > 100000:
            high_exec_time_stages.append(stage_id)
        if jvm_gc_time > 5000:
            high_gc_time_stages.append(stage_id)
        if shuffle_read_time > 1000 or shuffle_write_time > 1000:
            high_shuffle_time_stages.append(stage_id)

    df_stage_metrics = pd.DataFrame(stage_metrics)

    df_stage_metrics['is_cpu_bound'] = df_stage_metrics['executorCpuTime'] / df_stage_metrics['executorRunTime'] > 0.8
    df_stage_metrics['is_memory_bound'] = df_stage_metrics['jvmGCTime'] / df_stage_metrics['executorRunTime'] > 0.1
    df_stage_metrics['is_network_bound'] = (df_stage_metrics['shuffleFetchWaitTime'] + df_stage_metrics['shuffleWriteTime']) / df_stage_metrics['stageDuration'] > 0.5

    df_stage_metrics['is_spill'] = (df_stage_metrics['diskBytesSpilled'] + df_stage_metrics['memoryBytesSpilled']) > 0
    df_stage_metrics['is_skew'] = df_stage_metrics['executorDeserializeTime'] / df_stage_metrics['executorRunTime'] > 0.2
    df_stage_metrics['is_shuffle'] = (df_stage_metrics['shuffleFetchWaitTime'] + df_stage_metrics['shuffleWriteTime']) > 1000
    df_stage_metrics['is_serialization'] = df_stage_metrics['resultSerializationTime'] / df_stage_metrics['stageDuration'] > 0.1
    df_stage_metrics['is_storage'] = df_stage_metrics['peakExecutionMemory'] / (1024 * 1024 * 1024) > 1

    df_stage_metrics['stageDuration_minutes'] = df_stage_metrics['stageDuration'] / 60000
    df_stage_metrics['executorRunTime_minutes'] = df_stage_metrics['executorRunTime'] / 60000
    df_stage_metrics['executorCpuTime_minutes'] = df_stage_metrics['executorCpuTime'] / 60000
    df_stage_metrics['executorDeserializeTime_minutes'] = df_stage_metrics['executorDeserializeTime'] / 60000
    df_stage_metrics['executorDeserializeCpuTime_minutes'] = df_stage_metrics['executorDeserializeCpuTime'] / 60000
    df_stage_metrics['resultSerializationTime_minutes'] = df_stage_metrics['resultSerializationTime'] / 60000
    df_stage_metrics['jvmGCTime_minutes'] = df_stage_metrics['jvmGCTime'] / 60000
    df_stage_metrics['shuffleFetchWaitTime_minutes'] = df_stage_metrics['shuffleFetchWaitTime'] / 60000
    df_stage_metrics['shuffleWriteTime_minutes'] = df_stage_metrics['shuffleWriteTime'] / 60000

    df_stage_metrics['resultSize_MBs'] = df_stage_metrics['resultSize'] / (1024 * 1024)
    df_stage_metrics['diskBytesSpilled_MBs'] = df_stage_metrics['diskBytesSpilled'] / (1024 * 1024)
    df_stage_metrics['memoryBytesSpilled_MBs'] = df_stage_metrics['memoryBytesSpilled'] / (1024 * 1024)
    df_stage_metrics['peakExecutionMemory_MBs'] = df_stage_metrics['peakExecutionMemory'] / (1024 * 1024)
    df_stage_metrics['bytesRead_MBs'] = df_stage_metrics['bytesRead'] / (1024 * 1024)
    df_stage_metrics['bytesWritten_MBs'] = df_stage_metrics['bytesWritten'] / (1024 * 1024)
    df_stage_metrics['shuffleTotalBytesRead_MBs'] = df_stage_metrics['shuffleTotalBytesRead'] / (1024 * 1024)
    df_stage_metrics['shuffleLocalBytesRead_MBs'] = df_stage_metrics['shuffleLocalBytesRead'] / (1024 * 1024)
    df_stage_metrics['shuffleRemoteBytesRead_MBs'] = df_stage_metrics['shuffleRemoteBytesRead'] / (1024 * 1024)
    df_stage_metrics['shuffleRemoteBytesReadToDisk_MBs'] = df_stage_metrics['shuffleRemoteBytesReadToDisk'] / (1024 * 1024)
    df_stage_metrics['shuffleBytesWritten_MBs'] = df_stage_metrics['shuffleBytesWritten'] / (1024 * 1024)

    df_stage_metrics = df_stage_metrics.sort_values(by=['executorRunTime', 'jvmGCTime', 'shuffleFetchWaitTime', 'shuffleWriteTime'], ascending=False)

    return df_stage_metrics, high_exec_time_stages, high_gc_time_stages, high_shuffle_time_stages


def analyze_aggregated_metrics(file_path):
    with open(file_path) as f:
        aggregated_metrics = json.load(f)

    metrics_summary = {
        "numStages": aggregated_metrics.get("numStages", 0),
        "numTasks": aggregated_metrics.get("numTasks", 0),
        "elapsedTime (seconds)": aggregated_metrics.get("elapsedTime", 0) / 1000,
        "elapsedTime (minutes)": aggregated_metrics.get("elapsedTime", 0) / 60000,
        "executorRunTime (seconds)": aggregated_metrics.get("executorRunTime", 0) / 1000,
        "executorRunTime (minutes)": aggregated_metrics.get("executorRunTime", 0) / 60000,
        "executorCpuTime (seconds)": aggregated_metrics.get("executorCpuTime", 0) / 1000,
        "executorCpuTime (minutes)": aggregated_metrics.get("executorCpuTime", 0) / 60000,
        "jvmGCTime (seconds)": aggregated_metrics.get("jvmGCTime", 0) / 1000,
        "jvmGCTime (minutes)": aggregated_metrics.get("jvmGCTime", 0) / 60000,
        "peakExecutionMemory (MBs)": aggregated_metrics.get("peakExecutionMemory", 0) / (1024 * 1024),
        "shuffleTotalBytesRead (MBs)": aggregated_metrics.get("shuffleTotalBytesRead", 0) / (1024 * 1024),
        "shuffleBytesWritten (MBs)": aggregated_metrics.get("shuffleBytesWritten", 0) / (1024 * 1024),
        "recordsRead": aggregated_metrics.get("recordsRead", 0),
        "bytesRead (MBs)": aggregated_metrics.get("bytesRead", 0) / (1024 * 1024),
        "recordsWritten": aggregated_metrics.get("recordsWritten", 0),
        "bytesWritten (MBs)": aggregated_metrics.get("bytesWritten", 0) / (1024 * 1024),
    }

    return metrics_summary


def output_analysis(stage_metrics_file, aggregated_metrics_file, output_path):
    df_stage_metrics, high_exec_time_stages, high_gc_time_stages, high_shuffle_time_stages = analyze_stage_metrics(
        stage_metrics_file)
    metrics_summary = analyze_aggregated_metrics(aggregated_metrics_file)

    with open(output_path, 'w') as f:
        f.write("Aggregated Metrics Summary\n")
        f.write("=" * 20 + "\n\n")
        for key, value in metrics_summary.items():
            f.write(f"{key}\t{value}\n")

        f.write("\nStage Metrics Report\n")
        f.write("=" * 20 + "\n\n")
        f.write(
            "Stage ID\tStage Duration (ms)\tExecutor Run Time (ms)\tExecutor CPU Time (ms)\tJVM GC Time (ms)\tShuffle Fetch Wait Time (ms)\tShuffle Write Time (ms)\tPeak Execution Memory (bytes)\tRecords Read\tBytes Read (bytes)\tRecords Written\tBytes Written (bytes)\tCPU Bound\tMemory Bound\tNetwork Bound\tSpill\tSkew\tShuffle\tSerialization\tStorage\n")
        for index, row in df_stage_metrics.iterrows():
            f.write(
                f"{row['stageId']}\t{row['stageDuration']}\t{row['executorRunTime']}\t{row['executorCpuTime']}\t{row['jvmGCTime']}\t{row['shuffleFetchWaitTime']}\t{row['shuffleWriteTime']}\t{row['peakExecutionMemory']}\t{row['recordsRead']}\t{row['bytesRead']}\t{row['recordsWritten']}\t{row['bytesWritten']}\t{row['is_cpu_bound']}\t{row['is_memory_bound']}\t{row['is_network_bound']}\t{row['is_spill']}\t{row['is_skew']}\t{row['is_shuffle']}\t{row['is_serialization']}\t{row['is_storage']}\n")


stage_metrics_file = '/Users/luanmorenomaciel/GitHub/tuning-spark-app/metrics/elt-rides-fhvhv-py-strawberry-owshq/stagemetrics/part-00000-2b278241-4858-4a52-929a-f98a5cb429bf-c000.json'
aggregated_metrics_file = '/Users/luanmorenomaciel/GitHub/tuning-spark-app/metrics/elt-rides-fhvhv-py-strawberry-owshq/stagemetrics_agg/part-00000-64426906-7d59-452c-b6f0-dd99b032edd1-c000.json'

output_path = 'metrics_report.txt'
output_analysis(stage_metrics_file, aggregated_metrics_file, output_path)
