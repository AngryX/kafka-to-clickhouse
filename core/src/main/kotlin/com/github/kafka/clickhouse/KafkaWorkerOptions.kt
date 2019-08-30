package com.github.kafka.clickhouse

data class KafkaWorkerOptions(
        val name: String,
        val pollTimeout: Long = 1000,
        val topics: Collection<String>
)