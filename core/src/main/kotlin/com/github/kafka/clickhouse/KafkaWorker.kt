package com.github.kafka.clickhouse

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

//todo: reset offset in case of exception
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

    val commitCallback = OffsetCommitCallback { offsets, e ->
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
        closed.set(true)
    }

    override fun run() {
        log.info("KafkaWorker is run")
        try{
            while(closed.get()){
                val ready = processor.isReady()
                if(ready && paused.compareAndSet(false, true)){
                    consumer.resume(consumer.paused())
                }
                //todo: add latency metric
                //todo: add batch size metric
                val records = consumer.poll(options.pollTimeout)

                processor.handle(records.map { it.value() })

                if(ready){
                    consumer.commitAsync(commitCallback)
                } else {
                    //todo: add counter
                    log.info("KafkaWorker is getting sleep for a while")
                    if(paused.compareAndSet(true, false)){
                        consumer.pause(partitionsList.get())
                    }
                }
            }
        } catch(e: WakeupException){
            log.info("KafkaWorker was waked up")
        } catch(t: Throwable){
            log.error("Unknown error in KafkaWorker", t)
        } finally {
            commitBeforeClosing()
            consumer.close()
            log.info("KafkaWorker is closed")
        }
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
        var current = this.partitions.get()
        while (!this.partitions.compareAndSet(current, current.plus(partitions))){
            current = this.partitions.get()
        }
        log.info("Partitions assigned: {}", this.partitions.get().map { it.toString() })
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        log.info("Partitions to revoke: {}", partitions.map { it.toString() })
        var current = this.partitions.get()
        while (!this.partitions.compareAndSet(current, current.minus(partitions))){
            current = this.partitions.get()
        }
        log.info("Partitions assigned: {}", this.partitions.get().map { it.toString() })

    }
}