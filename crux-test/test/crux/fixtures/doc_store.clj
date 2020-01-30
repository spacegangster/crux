(ns crux.fixtures.doc-store
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.fixtures.api :as apif]
            [crux.io :as cio]
            [crux.index :as i])
  (:import java.io.Closeable))

(defrecord InMemDocumentStore [docs]
  Closeable
  (close [_])

  db/ObjectStore
  (put-objects [this id-and-docs]
    (doseq [[content-hash doc] id-and-docs]
      (log/debug "Storing" (cio/pr-edn-str content-hash))
      (swap! docs assoc content-hash doc)))
  (get-single-object [this _ k]
    (get (db/get-objects this nil [k]) k))
  (get-objects [this _ ids]
    (log/debug "Fetching" (cio/pr-edn-str ids))
    (into {}
          (for [id ids
                :let [doc (get @docs id)]
                :when (not (i/evicted-doc? doc))]
            [id doc])))
  (known-keys? [this _ ks]
    (every? (fn [k] (contains? @docs k)) ks)))

(def document-store
  {:start-fn (fn [_ _] (->InMemDocumentStore (atom {})))})

(defn with-remote-doc-store-opts [f]
  (apif/with-opts {:crux.node/object-store 'crux.fixtures.doc-store/document-store
                   :crux.kafka/doc-indexing-consumer 'crux.kafka/doc-indexing-from-tx-topic-consumer} f))
