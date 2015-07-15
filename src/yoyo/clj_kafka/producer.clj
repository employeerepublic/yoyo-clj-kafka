(ns yoyo.clj-kafka.producer
  (:require [clojure.tools.logging :as log]
            [clj-kafka.zk :as zk]
            [clj-kafka.new.producer :as kprod]))

(defn with-kafka-producer [{:keys [producer-opts]} f]
  (log/infof "Starting clj-kafka producer for producer-opts: %s" (prn-str producer-opts))
  (let [zookeeper-connect (:zookeeper.connect producer-opts)
        producer-opts (dissoc producer-opts :zookeeper.connect)
        producer-opts (into {} (map (fn [[k v]] [(name k) v]) producer-opts))
        broker-list (zk/broker-list (zk/brokers {"zookeeper.connect" zookeeper-connect}))
        producer (kprod/producer (merge producer-opts {"bootstrap.servers" broker-list})
                                 (kprod/byte-array-serializer)
                                 (kprod/byte-array-serializer))]
    (log/infof "discovered kafka servers: %s" broker-list)
    (log/info "Started clj-kafka producer.")
    (try
      (f producer)
      (finally
        (log/info "Stopping clj-kafka producer...")
        (.close producer)
        (log/info "Stopped clj-kafka producer.")))))
