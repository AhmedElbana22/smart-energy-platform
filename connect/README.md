# Kafka Connect / HDFS sink decision

The Compose profile starts a Kafka Connect worker. It does not install an HDFS sink plugin yet.

Do not silently install the deprecated HDFS 2 Sink against an HDFS 3 cluster. The HDFS 3 Sink is a separate connector with licensing requirements. Confirm connector compatibility, licence constraints, Java version, Hadoop version, serialization format, and file-rotation settings before committing a plugin image and connector config.

The chosen connector config should write raw `meter-readings` events to an HDFS Bronze path and should document how it handles schema changes, retries, file commits, and replay. Test with a few records before sending high-volume data.
