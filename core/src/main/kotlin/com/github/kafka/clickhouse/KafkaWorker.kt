package com.github.kafka.clickhouse

import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class KafkaWorker<K, V>(
        private val consumer: KafkaConsumer<K, V>,
        private val processor: RecordsProcessor<V>,
        private val options: KafkaWorkerOptions
): Worker {

    private val log = LoggerFactory.getLogger(KafkaWorker::class.java)

    // todo: add state to metrics
    private val started = AtomicBoolean(false)
    private val closed = AtomicBoolean(false)
    private val paused = AtomicBoolean(false)
    private val partitionsList = PartitionsList()

    private val commitCallback = OffsetCommitCallback { offsets, e ->
        if(e != null){
            //todo: add metric
            log.error("Error while committing offsets {}", offsets, e)
        } else {
            log.debug("{} are committed", offsets)
        }
    }
    override fun start() {
        if(started.compareAndSet(false, true)){
            consumer.subscribe(options.topics, partitionsList)
        }
    }

    override fun close() {
        consumer.wakeup()
    }

    override fun run() {
        log.info("KafkaWorker is run")
        try{
            while(true){
                val ready = processor.isReady()
                if(ready && paused.compareAndSet(true, false)){
                    consumer.resume(consumer.paused())
                }

                pollAndHandle()

                if(ready){
                    consumer.commitAsync(commitCallback)
                } else if(paused.compareAndSet(false, true)) {
                    //todo: add counter
                    log.info("KafkaWorker is getting sleep for a while")
                    consumer.pause(consumer.assignment())
                }
            }
        } catch(e: WakeupException){
            log.info("KafkaWorker was waked up")
        } catch(t: Throwable){
            log.error("Unknown error in KafkaWorker", t)
        } finally {
            commitBeforeClosing()
            consumer.close()
            closed.set(true)
            log.info("KafkaWorker is closed")
        }
    }

    private fun pollAndHandle(){
        //todo: add latency metric
        //todo: add batch size metric
        val records = consumer.poll(options.pollTimeout)
        try{
            processor.handle(records.map { it.value() })
        } catch(e: Exception){
            //todo: error metric
            log.error("Error while handling data", e)
            seekToCurrent(records)

        }


    }

    private fun seekToCurrent(records: ConsumerRecords<K, V>) {
        records.groupBy({ r -> TopicPartition(r.topic(), r.partition()) }) { r ->
            r.offset()
        }.mapValues { it.value.min() ?: 0 }
                .forEach { partition, offset -> consumer.seek(partition, offset) }
    }

    private fun commitBeforeClosing()  = try {
        consumer.commitSync()
    } catch(e: Exception){
        log.error("Unexpected error while committing offsets", e)
    }

}

class PartitionsList: ConsumerRebalanceListener {

    private val log = LoggerFactory.getLogger(PartitionsList::class.java)

    private val partitions = AtomicReference<Set<TopicPartition>>(emptySet())

    fun get() = partitions.get()

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        log.info("Partitions to assign: {}", partitions.map { it.toString() })
        this.partitions.set(partitions.toSet())
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        log.info("Partitions to revoke: {}", partitions.map { it.toString() })
        this.partitions.set(emptySet())
    }
}