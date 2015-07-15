(ns yoyo.clj-kafka.consumer
  (:require [clojure.tools.logging :as log]
            [clj-kafka.zk :as zk]
            [clj-kafka.consumer.zk :as kcon]))

(defn with-kafka-consumer [{:keys [consumer-opts]} f]
  (log/infof "Starting clj-kafka consumer for consumer-opts: %s" (prn-str consumer-opts))
  (let [consumer-opts (into {} (map (fn [[k v]] [(name k) v]) consumer-opts))
        consumer (kcon/consumer consumer-opts)]
    (log/info "Started clj-kafka consumer.")
    (try
      (f consumer)
      (finally
        (log/info "Stopping clj-kafka consumer...")
        (.close consumer)
        (log/info "Stopped clj-kafka consumer.")))))
