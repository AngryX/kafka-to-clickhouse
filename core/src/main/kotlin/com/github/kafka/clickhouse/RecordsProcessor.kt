package com.github.kafka.clickhouse

interface RecordsProcessor<V> {

    fun handle(records: Collection<V>)

    fun isReady(): Boolean = true
}