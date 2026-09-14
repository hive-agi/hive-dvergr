(ns hive-dvergr.addon
  "IAddon composition root for the dvergr mini-experiment."
  (:require [hive-addon.protocol :as addon]
            [hive-dsl.result :as r]
            [hive-dvergr.datahike-ledger :as ledger]
            [hive-dvergr.hive-agent :as agent]
            [hive-dvergr.ports :as ports]
            [hive-dvergr.runtime :as runtime]
            [hive-dvergr.schema :as schema]
            [hive-dvergr.sandbox :as sandbox]
            [hive-spi.notify :as notify]))

(defn scripted-runner
  "Deterministic default for installation smoke tests; hosts inject real runners."
  [request]
  {:task (get-in request [:run/input :task])
   :input (:run/input request)
   :runtime :dvergr-generation-handle})

(defn- config-value [config key default]
  (or (get-in config [:addon/config key])
      (get config key)
      default))

(defn- configured-runner [defaults config]
  (let [settings (merge defaults config (:addon/config config))]
    (if (contains? settings :sandbox)
      (sandbox/runner (:sandbox settings))
      (or (:runner settings) scripted-runner))))

(defn- tool-result [value]
  {:content [{:type "text" :text (pr-str value)}]})

(defn- configured-publisher [defaults config]
  (let [settings (merge defaults config (:addon/config config))
        publish! (or (:publish! settings) (constantly nil))
        backends (:notify/backends settings)]
    (doseq [backend backends]
      (when-not (satisfies? notify/INotify backend)
        (throw (ex-info "Notification backend must implement INotify" {}))))
    (fn [event]
      (try
        (publish! event)
        (finally
          (doseq [backend backends]
            (try
              (when (and (notify/backend-available? backend)
                         (notify/accepts? backend (:event/type event)))
                (notify/notify! backend
                  {:event-type (:event/type event)
                   :summary (str (:agent/id event) " " (:event/type event))
                   :body (str "Run " (:run/id event))
                   :urgency :normal
                   :level (if (= :run/failed (:event/type event)) :error :info)
                   :agent-id (:agent/id event)
                   :run/id (:run/id event)
                   :timestamp (:event/at event)}))
              (catch Exception _ nil))))))))

(defrecord HiveDvergrAddon [state defaults]
  addon/IAddon

  (addon-id [_] "hive.dvergr")
  (addon-type [_] :native)
  (capabilities [_] #{:tools :health-reporting :agent-runtime :datahike})

  (initialize! [_ config]
    (if @state
      {:success? true :already-initialized? true}
      (let [db-cfg (config-value config :db-cfg
                                 (or (:db-cfg defaults)
                                     (ledger/default-config)))
            supplied-ledger (config-value config :ledger (:ledger defaults))
            ledger-result (if supplied-ledger
                            (r/ok supplied-ledger)
                            (ledger/make-ledger db-cfg))]
        (if-not (r/ok? ledger-result)
          {:success? false :errors [(pr-str ledger-result)]}
          (try
            (schema/install!)
            (let [run-ledger (:ok ledger-result)
                  runner (configured-runner defaults config)
                  publisher (configured-publisher defaults config)
                  run-runtime (runtime/make-runtime
                               {:runner runner
                                :ledger run-ledger
                                :publish! publisher})
                  integration (agent/install-protocol!)]
              (reset! state {:runtime run-runtime
                             :ledger run-ledger
                             :owns-ledger? (nil? supplied-ledger)
                             :integration integration})
              {:success? true
               :metadata {:integration integration
                          :evidence-backend :datahike}})
            (catch Throwable t
              (schema/uninstall!)
              (when (and (nil? supplied-ledger) (r/ok? ledger-result))
                (ports/close-ledger! (:ok ledger-result)))
              {:success? false :errors [(ex-message t)]}))))))

  (shutdown! [_]
    (when-let [{:keys [runtime ledger owns-ledger?]} @state]
      (ports/shutdown-runtime! runtime)
      (when owns-ledger? (ports/close-ledger! ledger)))
    (schema/uninstall!)
    (reset! state nil)
    nil)

  (tools [_]
    [{:name "dvergr_run"
      :description "Run one task through dvergr and recover its durable Datahike outcome"
      :inputSchema {:type "object"
                    :properties {"task" {:type "string"}
                                 "code" {:type "string"
                                         :description "Clojure source for the configured cljw sandbox"}
                                 "agent_id" {:type "string"}
                                 "timeout_ms" {:type "integer" :minimum 1}}
                    :required ["task"]}
      :handler
      (fn [params]
        (if-let [run-runtime (:runtime @state)]
          (let [run-id (str (random-uuid))
                request {:run/id run-id
                         :task/id (str (random-uuid))
                         :agent/id (or (get params "agent_id")
                                       (:agent_id params)
                                       "hive-dvergr")
                         :run/attempt 0
                         :run/input (cond-> {:task (or (get params "task") (:task params))}
                                      (or (contains? params "code") (contains? params :code))
                                      (assoc :code (or (get params "code") (:code params))))}
                result (runtime/run-sync!
                        run-runtime request
                        {:timeout-ms (long (or (get params "timeout_ms")
                                               (:timeout_ms params)
                                               60000))})]
            (tool-result result))
          (tool-result {:error :hive-dvergr/not-initialized}))) }])

  (schema-extensions [_] [])

  (health [_]
    (if-let [{:keys [integration]} @state]
      {:status (if (:installed? integration) :ok :degraded)
       :details {:evidence-backend :datahike
                 :integration integration}}
      {:status :down :details {:reason :not-initialized}}))

  (excluded-tools [_] #{})

  (hooks [_]
    (if-let [run-runtime (:runtime @state)]
      {:ag/run-runtime run-runtime
       :ag/loop-factory (agent/make-loop-factory run-runtime)
       :ag/loop-backend (fn [] (agent/make-backend run-runtime))}
      {})))

(defn make-addon
  ([] (make-addon {}))
  ([defaults]
   (->HiveDvergrAddon (atom nil) defaults)))

(defn init-as-addon!
  "Manifest entrypoint. Returns an uninitialized IAddon instance."
  []
  (make-addon))

(defn addon-ctor
  "Pure mount constructor. Preserve host configuration and notification ports."
  [config]
  (make-addon (merge config (:addon/config config))))