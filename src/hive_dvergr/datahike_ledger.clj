(ns hive-dvergr.datahike-ledger
  "Datahike implementation of the durable run ledger."
  (:require [clojure.edn :as edn]
            [datahike.api :as d]
            [hive-dsl.result :as r]
            [hive-dvergr.ports :as ports]
            [hive-dvergr.schema :as schema]))

(def ledger-schema
  [{:db/ident :run/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :run/task-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :run/agent-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :run/attempt
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :run/status
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :run/created-at
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :run/updated-at
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :run/completed-at
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :run/result-edn
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :run/result-hash
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :event/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :event/run
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :event/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :event/at
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :event/sequence
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :event/payload-edn
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(defn default-config
  ([] (default-config ".hive-dvergr/datahike"))
  ([path]
   {:store {:backend :file
            :id (java.util.UUID/nameUUIDFromBytes (.getBytes (str path)))
            :path path}
    :schema-flexibility :write}))

(defn memory-config
  "Create an isolated in-memory config for deterministic tests and REPL spikes."
  []
  {:store {:backend :mem :id (random-uuid)}
   :schema-flexibility :write})

(defn- storage-event [event]
  {:event/id (:event/id event)
   :event/run [:run/id (:run/id event)]
   :event/type (:event/type event)
   :event/at (:event/at event)
   :event/sequence (:run/sequence event)
   :event/payload-edn (pr-str (:event/payload event))})

(defn- hydrate-event [run ent]
  {:event/id (:event/id ent)
   :event/type (:event/type ent)
   :event/at (:event/at ent)
   :run/id (:run/id run)
   :task/id (:run/task-id run)
   :agent/id (:run/agent-id run)
   :run/attempt (:run/attempt run)
   :run/sequence (:event/sequence ent)
   :event/payload (edn/read-string (:event/payload-edn ent))})

(defn- pull-run [conn run-id]
  (d/q '[:find (pull ?r [*]) .
         :in $ ?run-id
         :where [?r :run/id ?run-id]]
       (d/db conn) run-id))

(defn- pull-events [conn run-id]
  (d/q '[:find [(pull ?e [*]) ...]
         :in $ ?run-id
         :where
         [?r :run/id ?run-id]
         [?e :event/run ?r]]
       (d/db conn) run-id))

(defrecord DatahikeRunLedger [conn cfg closed?]
  ports/IRunLedger

  (begin-run! [_ request]
    (if-not (schema/valid? :hive-dvergr/run-request request)
      (r/err :hive-dvergr/invalid-run-request {:request request})
      (try
        (let [now (System/currentTimeMillis)]
          (d/transact conn
                      [{:run/id (:run/id request)
                        :run/task-id (:task/id request)
                        :run/agent-id (:agent/id request)
                        :run/attempt (:run/attempt request)
                        :run/status :run/submitted
                        :run/created-at now
                        :run/updated-at now}])
          (r/ok request))
        (catch Throwable t
          (r/err :hive-dvergr/datahike-begin-failed
                 {:message (ex-message t) :run/id (:run/id request)})))))

  (append-event! [_ event]
    (if-not (schema/valid? :hive-dvergr/run-event event)
      (r/err :hive-dvergr/invalid-run-event {:event event})
      (try
        (d/transact conn [(storage-event event)
                          {:run/id (:run/id event)
                           :run/status (case (:event/type event)
                                         :run/submitted :run/submitted
                                         :run/started :run/running
                                         :run/completed :run/completed
                                         :run/failed :run/failed
                                         :run/cancelled :run/cancelled
                                         :run/interrupted :run/interrupted
                                         :run/cancel-requested :run/running)
                           :run/updated-at (:event/at event)}])
        (r/ok event)
        (catch Throwable t
          (r/err :hive-dvergr/datahike-event-failed
                 {:message (ex-message t) :event/id (:event/id event)})))))

  (record-outcome! [_ outcome]
    (if-not (schema/valid? :hive-dvergr/run-outcome outcome)
      (r/err :hive-dvergr/invalid-run-outcome {:outcome outcome})
      (try
        (d/transact conn
                    [{:run/id (:run/id outcome)
                      :run/status (:run/status outcome)
                      :run/result-edn (pr-str (:run/result outcome))
                      :run/result-hash (:run/result-hash outcome)
                      :run/completed-at (:run/completed-at outcome)
                      :run/updated-at (:run/completed-at outcome)}])
        (r/ok outcome)
        (catch Throwable t
          (r/err :hive-dvergr/datahike-outcome-failed
                 {:message (ex-message t) :run/id (:run/id outcome)})))))

  (run-events [_ run-id]
    (try
      (if-let [run (pull-run conn run-id)]
        (r/ok (->> (pull-events conn run-id)
                   (map #(hydrate-event run %))
                   (sort-by :run/sequence)
                   vec))
        (r/err :hive-dvergr/run-not-found {:run/id run-id}))
      (catch Throwable t
        (r/err :hive-dvergr/datahike-query-failed
               {:message (ex-message t) :run/id run-id}))))

  (run-record [_ run-id]
    (try
      (if-let [run (pull-run conn run-id)]
        (r/ok (cond-> {:run/id (:run/id run)
                       :task/id (:run/task-id run)
                       :agent/id (:run/agent-id run)
                       :run/attempt (:run/attempt run)
                       :run/status (:run/status run)
                       :run/created-at (:run/created-at run)
                       :run/updated-at (:run/updated-at run)}
                (:run/completed-at run)
                (assoc :run/completed-at (:run/completed-at run))
                (:run/result-edn run)
                (assoc :run/result (edn/read-string (:run/result-edn run)))
                (:run/result-hash run)
                (assoc :run/result-hash (:run/result-hash run))))
        (r/err :hive-dvergr/run-not-found {:run/id run-id}))
      (catch Throwable t
        (r/err :hive-dvergr/datahike-query-failed
               {:message (ex-message t) :run/id run-id}))))

  (close-ledger! [_]
    (when (compare-and-set! closed? false true)
      (try (d/release conn) (catch Throwable _ nil)))
    (r/ok {:closed? true :config cfg})))

(defn make-ledger
  "Open or create a Datahike run ledger. Returns Result<DatahikeRunLedger>."
  ([] (make-ledger (default-config)))
  ([cfg]
   (try
     (let [new? (not (d/database-exists? cfg))]
       (when new? (d/create-database cfg))
       (let [conn (d/connect cfg)]
         (when new? (d/transact conn ledger-schema))
         (r/ok (->DatahikeRunLedger conn cfg (atom false)))))
     (catch Throwable t
       (r/err :hive-dvergr/datahike-open-failed
              {:message (ex-message t) :config cfg})))))
