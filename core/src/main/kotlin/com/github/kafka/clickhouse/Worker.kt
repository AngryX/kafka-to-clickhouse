package com.github.kafka.clickhouse

interface Worker: Runnable, AutoCloseable{
    fun start()

}