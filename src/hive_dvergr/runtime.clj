(ns hive-dvergr.runtime
  "dvergr GenerationHandle adapter with Datahike-first evidence semantics."
  (:require [dvergr.core :as d]
            [dvergr.discourse :as discourse]
            [dvergr.discourse.generation :as generation]
            [hive-dsl.result :as r]
            [hive-dvergr.hash :as hash]
            [hive-dvergr.ports :as ports]
            [hive-dvergr.schema :as schema]
            [org.replikativ.spindel.core :as sp]
            [org.replikativ.spindel.engine.core :as ec])
  (:import [java.nio.charset StandardCharsets]
           [java.util UUID]))

(def terminal-statuses
  #{:run/completed :run/failed :run/cancelled :run/interrupted})

(defrecord RunHandle [run-id task-id agent-id attempt])

(defn- handle-value [request]
  {:run/id (:run/id request)
   :task/id (:task/id request)
   :agent/id (:agent/id request)
   :run/attempt (:run/attempt request)})

(defn- room-id [run-id]
  (keyword
   (str "hive-dvergr-"
        (UUID/nameUUIDFromBytes (.getBytes run-id StandardCharsets/UTF_8)))))

(defn- await-generation [ctx deferred]
  (binding [ec/*execution-context* ctx]
    @(sp/spin (sp/await deferred))))

(defn- terminal-record? [record]
  (contains? terminal-statuses (:run/status record)))

(defn- outcome-from-record [record]
  (select-keys record
               [:run/id :task/id :agent/id :run/attempt :run/status
                :run/result :run/result-hash :run/completed-at]))

(defn- normalize-result [value]
  (if (map? value) value {:value value}))

(defn- next-sequence! [runtime run-id]
  (get (swap! (:sequences runtime) update run-id (fnil inc -1)) run-id))

(defn- event-recorder [runtime request]
  (fn [event-type payload]
    (let [event {:event/id (str (random-uuid))
                 :event/type event-type
                 :event/at (long ((:clock runtime)))
                 :run/id (:run/id request)
                 :task/id (:task/id request)
                 :agent/id (:agent/id request)
                 :run/attempt (:run/attempt request)
                 :run/sequence (next-sequence! runtime (:run/id request))
                 :event/payload payload}
          committed (ports/append-event! (:ledger runtime) event)]
      (when (r/ok? committed)
        (try ((:publish! runtime) event) (catch Throwable _ nil)))
      committed)))

(defn- outcome-committer [runtime request record-event!]
  (fn [status value]
    (let [result (normalize-result value)
          outcome {:run/id (:run/id request)
                   :task/id (:task/id request)
                   :agent/id (:agent/id request)
                   :run/attempt (:run/attempt request)
                   :run/status status
                   :run/result result
                   :run/result-hash (hash/sha256 result)
                   :run/completed-at (long ((:clock runtime)))}
          committed (ports/record-outcome! (:ledger runtime) outcome)]
      (when (r/ok? committed)
        (record-event! status {:result-hash (:run/result-hash outcome)}))
      outcome)))

(defn- recover-outcome [runtime run-id fallback-status]
  (let [record (ports/run-record (:ledger runtime) run-id)]
    (if (r/ok? record)
      (outcome-from-record (:ok record))
      {:run/id run-id :run/status fallback-status})))

(defn- execute-run [runtime request terminal? record-event! commit-outcome!]
  (record-event! :run/started {})
  (try
    (let [result ((:runner runtime) request)]
      (if (compare-and-set! terminal? false true)
        (commit-outcome! :run/completed result)
        (recover-outcome runtime (:run/id request) :run/cancelled)))
    (catch Throwable t
      (if (compare-and-set! terminal? false true)
        (commit-outcome! :run/failed
                         {:error (ex-message t)
                          :exception (str (class t))})
        (recover-outcome runtime (:run/id request) :run/failed)))))

(defn- start-run* [runtime external-request]
  (if @(:closed? runtime)
    (r/err :hive-dvergr/runtime-closed)
    (r/bind
     (schema/promote-request external-request)
     (fn [request]
       (r/bind
        (ports/begin-run! (:ledger runtime) request)
        (fn [_]
          (let [terminal? (atom false)
                record-event! (event-recorder runtime request)
                submitted (record-event! :run/submitted {})]
            (if-not (r/ok? submitted)
              submitted
              (let [room (d/room (room-id (:run/id request)))
                    commit-outcome! (outcome-committer runtime request record-event!)
                    handle (map->RunHandle
                            {:run-id (:run/id request)
                             :task-id (:task/id request)
                             :agent-id (:agent/id request)
                             :attempt (:run/attempt request)})
                    gen (generation/future-handle
                         (:ctx room)
                         #(execute-run runtime request terminal?
                                       record-event! commit-outcome!))]
                (swap! (:active runtime) assoc (:run/id request)
                       {:request request
                        :handle handle
                        :generation gen
                        :room room
                        :terminal? terminal?
                        :record-event! record-event!
                        :commit-outcome! commit-outcome!})
                (r/ok (handle-value request)))))))))))

(defn- cleanup-active! [runtime run-id]
  (when-let [{:keys [room]} (get @(:active runtime) run-id)]
    (discourse/close-room! room)
    (swap! (:active runtime) dissoc run-id)))

(defn- await-run* [runtime run-id {:keys [timeout-ms] :or {timeout-ms 60000}}]
  (let [record (ports/run-record (:ledger runtime) run-id)]
    (cond
      (and (r/ok? record) (terminal-record? (:ok record)))
      (do (cleanup-active! runtime run-id)
          (r/ok (outcome-from-record (:ok record))))

      (contains? @(:active runtime) run-id)
      (let [{:keys [generation room]} (get @(:active runtime) run-id)
            waiter (future (await-generation (:ctx room) (:done generation)))
            value (deref waiter timeout-ms ::timeout)]
        (if (= ::timeout value)
          (do (future-cancel waiter)
              (r/err :hive-dvergr/await-timeout
                     {:run/id run-id :timeout-ms timeout-ms}))
          (let [durable (ports/run-record (:ledger runtime) run-id)]
            (cleanup-active! runtime run-id)
            (if (and (r/ok? durable) (terminal-record? (:ok durable)))
              (r/ok (outcome-from-record (:ok durable)))
              (r/err :hive-dvergr/outcome-not-durable
                     {:run/id run-id :generation-value value})))))

      :else record)))

(defn- cancel-run* [runtime run-id]
  (if-let [{:keys [generation terminal? record-event! commit-outcome!]}
           (get @(:active runtime) run-id)]
    (if (compare-and-set! terminal? false true)
      (do
        (record-event! :run/cancel-requested {})
        ((:cancel! generation))
        (let [outcome (commit-outcome! :run/cancelled {:cancelled true})]
          (cleanup-active! runtime run-id)
          (r/ok outcome)))
      (let [record (ports/run-record (:ledger runtime) run-id)]
        (if (r/ok? record)
          (r/ok (outcome-from-record (:ok record)))
          record)))
    (let [record (ports/run-record (:ledger runtime) run-id)]
      (if (and (r/ok? record) (terminal-record? (:ok record)))
        (r/ok (outcome-from-record (:ok record)))
        (r/err :hive-dvergr/run-not-live {:run/id run-id})))))

(defrecord DvergrRunRuntime [runner ledger publish! clock active sequences closed?]
  ports/IRunRuntime
  (start-run! [this request] (start-run* this request))
  (await-run! [this run-id opts] (await-run* this run-id opts))
  (cancel-run! [this run-id] (cancel-run* this run-id))
  (run-status [_ run-id] (ports/run-record ledger run-id))
  (shutdown-runtime! [this]
    (when (compare-and-set! closed? false true)
      (doseq [run-id (keys @active)]
        (cancel-run* this run-id))
      (reset! active {}))
    (r/ok {:closed? true})))

(defn make-runtime
  "Construct a dvergr runtime around an injected deterministic or LLM runner."
  [{:keys [runner ledger publish! clock]
    :or {publish! (constantly nil)
         clock #(System/currentTimeMillis)}}]
  {:pre [(ifn? runner) (satisfies? ports/IRunLedger ledger)]}
  (->DvergrRunRuntime runner ledger publish! clock
                      (atom {}) (atom {}) (atom false)))

(defn run-sync!
  "Convenience boundary used by the addon tool and deterministic experiment."
  [runtime request opts]
  (let [started (ports/start-run! runtime request)]
    (if (r/ok? started)
      (ports/await-run! runtime (:run/id (:ok started)) opts)
      started)))
