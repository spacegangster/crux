(ns crux.kafka
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.kafka.consumer :as kc]
            [crux.node :as n]
            [crux.tx :as tx]
            [taoensso.nippy :as nippy]
            [crux.status :as status])
  (:import crux.db.DocumentStore
           crux.kafka.nippy.NippySerializer
           java.io.Closeable
           java.time.Duration
           [java.util Date Map]
           java.util.concurrent.ExecutionException
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata]
           org.apache.kafka.common.errors.TopicExistsException
           org.apache.kafka.common.TopicPartition))

(s/def ::bootstrap-servers string?)
(s/def ::group-id string?)
(s/def ::topic string?)
(s/def ::partitions pos-int?)
(s/def ::replication-factor pos-int?)

(s/def ::tx-topic ::topic)
(s/def ::doc-topic ::topic)
(s/def ::doc-partitions ::partitions)
(s/def ::create-topics boolean?)

(def default-producer-config
  {"enable.idempotence" "true"
   "acks" "all"
   "compression.type" "snappy"
   "key.serializer" (.getName NippySerializer)
   "value.serializer" (.getName NippySerializer)})

(def default-topic-config
  {"message.timestamp.type" "LogAppendTime"})

(def tx-topic-config
  {"retention.ms" (str Long/MAX_VALUE)})

(def doc-topic-config
  {"cleanup.policy" "compact"})

(defn- read-kafka-properties-file [f]
  (when f
    (with-open [in (io/reader (io/file f))]
      (cio/load-properties in))))

(defn- derive-kafka-config [{:keys [crux.kafka/bootstrap-servers
                                    crux.kafka/kafka-properties-file
                                    crux.kafka/kafka-properties-map]}]
  (merge {"bootstrap.servers" bootstrap-servers}
         (read-kafka-properties-file kafka-properties-file)
         kafka-properties-map))

(defn create-producer
  ^org.apache.kafka.clients.producer.KafkaProducer [config]
  (KafkaProducer. ^Map (merge default-producer-config config)))

(defn create-admin-client
  ^org.apache.kafka.clients.admin.AdminClient [config]
  (AdminClient/create ^Map config))

(defn create-topic [^AdminClient admin-client topic num-partitions replication-factor config]
  (let [new-topic (doto (NewTopic. topic num-partitions replication-factor)
                    (.configs (merge default-topic-config config)))]
    (try
      @(.all (.createTopics admin-client [new-topic]))
      (catch ExecutionException e
        (let [cause (.getCause e)]
          (when-not (instance? TopicExistsException cause)
            (throw e)))))))

(defn- ensure-topic-exists [admin-client topic topic-config partitions {:keys [crux.kafka/replication-factor
                                                                               crux.kafka/create-topics]}]
  (when create-topics
    (create-topic admin-client topic partitions replication-factor topic-config)))

(defn- ensure-tx-topic-has-single-partition [^AdminClient admin-client tx-topic]
  (let [name->description @(.all (.describeTopics admin-client [tx-topic]))]
    (assert (= 1 (count (.partitions ^TopicDescription (get name->description tx-topic)))))))

(defn tx-record->tx-log-entry [^ConsumerRecord record]
  {:crux.tx.event/tx-events (.value record)
   :crux.tx/tx-id (.offset record)
   :crux.tx/tx-time (Date. (.timestamp record))})

(defrecord KafkaTxLog [^KafkaProducer producer, ^KafkaConsumer latest-submitted-tx-consumer, tx-topic, kafka-config]
  Closeable
  (close [_])

  db/TxLog
  (submit-tx [this tx-ops]
    (try
      (let [tx-events (map tx/tx-op->tx-event tx-ops)
            content-hashes (->> (set (map c/new-id (mapcat tx/tx-op->docs tx-ops))))
            tx-send-future (->> (doto (ProducerRecord. tx-topic nil tx-events)
                                  (-> (.headers) (.add (str :crux.tx/docs)
                                                       (nippy/fast-freeze content-hashes))))
                                (.send producer))]
        (delay
         (let [record-meta ^RecordMetadata @tx-send-future]
           {:crux.tx/tx-id (.offset record-meta)
            :crux.tx/tx-time (Date. (.timestamp record-meta))})))))

  (open-tx-log [this from-tx-id]
    (let [tx-topic-consumer ^KafkaConsumer (kc/create-consumer (assoc kafka-config "enable.auto.commit" "false"))
          tx-topic-partition (TopicPartition. tx-topic 0)]
      (.assign tx-topic-consumer [tx-topic-partition])
      (if from-tx-id
        (.seek tx-topic-consumer tx-topic-partition (long from-tx-id))
        (.seekToBeginning tx-topic-consumer (.assignment tx-topic-consumer)))
      (db/->closeable-tx-log-iterator
       #(.close tx-topic-consumer)
       ((fn step []
          (when-let [records (seq (.poll tx-topic-consumer (Duration/ofMillis 1000)))]
            (concat (map tx-record->tx-log-entry records)
                    (step))))))))

  (latest-submitted-tx [this]
    (let [tx-tp (TopicPartition. tx-topic 0)
          end-offset (-> (.endOffsets latest-submitted-tx-consumer [tx-tp]) (get tx-tp))]
      (when (pos? end-offset)
        {:crux.tx/tx-id (dec end-offset)})))

  status/Status
  (status-map [_]
    {:crux.zk/zk-active?
     (try
       (boolean (.listTopics latest-submitted-tx-consumer))
       (catch Exception e
         (log/debug e "Could not list Kafka topics:")
         false))}))

(defrecord KafkaDocumentStore [^KafkaProducer producer, doc-topic]
  Closeable
  (close [_])

  db/DocumentStore
  (submit-docs [this id-and-docs]
    (doseq [[content-hash doc] id-and-docs]
      (->> (ProducerRecord. doc-topic content-hash doc)
           (.send producer)))
    (.flush producer)))

(defn- group-name []
  (str/trim (or (System/getenv "HOSTNAME")
                (System/getenv "COMPUTERNAME")
                (.toString (java.util.UUID/randomUUID)))))

(def default-options
  {::bootstrap-servers {:doc "URL for connecting to Kafka i.e. \"kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092\""
                        :default "localhost:9092"
                        :crux.config/type :crux.config/string}
   ::tx-topic {:doc "Kafka transaction topic"
               :default "crux-transaction-log"
               :crux.config/type :crux.config/string}
   ::doc-topic {:doc "Kafka document topic"
                :default "crux-docs"
                :crux.config/type :crux.config/string}
   ::doc-partitions {:doc "Partitions for document topic"
                     :default 1
                     :crux.config/type :crux.config/nat-int}
   ::create-topics {:doc "Create topics if they do not exist"
                    :default true
                    :crux.config/type :crux.config/boolean}
   ::replication-factor {:doc "Level of durability for Kafka"
                         :default 1
                         :crux.config/type :crux.config/nat-int}
   ::group-id {:doc "Kafka client group.id"
               :default (group-name)
               :crux.config/type :crux.config/string}
   ::kafka-properties-file {:doc "Used for supplying Kafka connection properties to the underlying Kafka API."
                            :crux.config/type :crux.config/string}
   ::kafka-properties-map {:doc "Used for supplying Kafka connection properties to the underlying Kafka API."
                           :crux.config/type [map? identity]}})

(defn accept-txes? [indexer ^ConsumerRecord tx-record]
  (let [content-hashes (->> (.lastHeader (.headers tx-record)
                                         (str :crux.tx/docs))
                            (.value)
                            (nippy/fast-thaw))
        ready? (db/docs-indexed? indexer content-hashes)
        {:crux.tx/keys [tx-time
                        tx-id]} (tx-record->tx-log-entry tx-record)]
    (if ready?
      (log/info "Ready for indexing of tx" tx-id (cio/pr-edn-str tx-time))
      (log/info "Delaying indexing of tx" tx-id (cio/pr-edn-str tx-time)))
    ready?))

(defn index-txes
  [indexer records]
  (doseq [record records]
    (let [{:keys [crux.tx.event/tx-events] :as record} (tx-record->tx-log-entry record)]
      (db/index-tx indexer (select-keys record [:crux.tx/tx-time :crux.tx/tx-id]) tx-events))))

(def tx-indexing-consumer
  {:start-fn (fn [{:keys [crux.kafka/admin-client crux.node/indexer]}
                  {::keys [tx-topic group-id] :as options}]
               (ensure-topic-exists admin-client tx-topic tx-topic-config 1 options)
               (ensure-tx-topic-has-single-partition admin-client tx-topic)
               (kc/start-indexing-consumer {:indexer indexer
                                            :offsets (kc/->TxOffset indexer)
                                            :kafka-config (derive-kafka-config options)
                                            :group-id group-id
                                            :topic tx-topic
                                            :accept-fn (partial accept-txes? indexer)
                                            :index-fn (partial index-txes indexer)}))
   :deps [:crux.node/indexer ::admin-client]
   :args default-options})

(defn index-documents
  [indexer records]
  (db/index-docs indexer (->> records
                              (into {} (map (fn [^ConsumerRecord record]
                                              [(c/new-id (.key record)) (.value record)]))))))

(def doc-indexing-consumer
  {:start-fn (fn [{:keys [crux.kafka/admin-client crux.node/indexer]}
                  {::keys [doc-topic doc-partitions group-id] :as options}]
               (ensure-topic-exists admin-client doc-topic doc-topic-config doc-partitions options)
               (kc/start-indexing-consumer {:indexer indexer
                                            :offsets (kc/->ConsumerOffsets indexer :crux.tx-log/consumer-state)
                                            :kafka-config (derive-kafka-config options)
                                            :group-id group-id
                                            :topic doc-topic
                                            :index-fn (partial index-documents indexer)}))
   :deps [:crux.node/indexer ::admin-client]
   :args default-options})

(defn index-documents-from-txes
  [indexer document-store records]
  (doseq [^ConsumerRecord tx-record records]
    (let [content-hashes (->> (.lastHeader (.headers tx-record)
                                           (str :crux.tx/docs))
                              (.value)
                              (nippy/fast-thaw))]
      (let [docs (db/fetch-docs document-store content-hashes)]
        (db/index-docs indexer docs)))))

(def doc-indexing-from-tx-topic-consumer
  {:start-fn (fn [{:keys [crux.node/indexer crux.node/document-store]}
                  {::keys [tx-topic doc-group-id] :as options}]
               (kc/start-indexing-consumer {:indexer indexer
                                            :offsets (kc/->ConsumerOffsets indexer :crux.tx-log/consumer-state)
                                            :kafka-config (derive-kafka-config options)
                                            :group-id doc-group-id
                                            :topic tx-topic
                                            :index-fn (partial index-documents-from-txes indexer document-store)}))
   :deps [:crux.node/indexer :crux.node/document-store ::tx-indexing-consumer]
   :args (assoc default-options
                ::doc-group-id {:doc "Kafka client group.id for ingesting documents using tx topic"
                                :default (str "documents-" (group-name))
                                :crux.config/type :crux.config/string})})

(def admin-client
  {:start-fn (fn [_ options]
               (create-admin-client (derive-kafka-config options)))
   :args default-options})

(def admin-wrapper
  {:start-fn (fn [{::keys [admin-client]} _]
               (reify Closeable
                 (close [_])))
   :deps [::admin-client]})

(def producer
  {:start-fn (fn [_ options]
               (create-producer (derive-kafka-config options)))
   :args default-options})

(def latest-submitted-tx-consumer
  {:start-fn (fn [_ options]
               (kc/create-consumer (derive-kafka-config options)))
   :args default-options})

(def tx-log
  {:start-fn (fn [{:keys [::producer ::latest-submitted-tx-consumer]}
                  {:keys [crux.kafka/tx-topic] :as options}]
               (->KafkaTxLog producer latest-submitted-tx-consumer tx-topic (derive-kafka-config options)))
   :deps [::producer ::latest-submitted-tx-consumer]
   :args default-options})

(def document-store
  {:start-fn (fn [{::keys [producer]} {:keys [crux.kafka/doc-topic] :as options}]
               (->KafkaDocumentStore producer doc-topic))
   :deps [::producer]
   :args default-options})

(def topology
  (merge n/base-topology
         {:crux.node/tx-log tx-log
          :crux.node/document-store document-store
          ::admin-client admin-client
          ::admin-wrapper admin-wrapper
          ::producer producer
          ::tx-indexing-consumer tx-indexing-consumer
          ::doc-indexing-consumer doc-indexing-consumer
          ::latest-submitted-tx-consumer latest-submitted-tx-consumer}))
